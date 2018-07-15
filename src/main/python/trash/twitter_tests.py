import socket
import sys
import requests
import requests_oauthlib
import json
import twitter
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import os
from tweepy import API
from tweepy import OAuthHandler
import tweepy

def connect_twitter():
    """
        Fonction de connection à Twitter.
        Note : Requiert les informations de connection fournies par l'API Twitter
    Return
        TwitterStream (cf. bibliothèque Twitter)
    """
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    client = API(auth)
    return client

api = connect_twitter()
public_tweets = api.home_timeline()
for tweet in public_tweets:
    print(tweet.text)

class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        print(status.text)

    def on_data(self, data):
        print(data)

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
myStream.filter(track=['#micron'])


def get_next_tweet(twitter_stream, i ):
    """
    Return : JSON
    """
    block    = False # True
    stream   = twitter_stream.statuses.sample(block=False)
    tweet_in = None
    while not tweet_in or 'delete' in tweet_in:
        tweet_in     = stream.next()
    return json.dumps(tweet_in)


def process_rdd_queue(twitter_stream, nb_tweets = 5):
    """
     Create a queue of RDDs that will be mapped/reduced one at a time in 1 second intervals.
    """
    rddQueue = []
    for i in range(nb_tweets):
        json_twt  = get_next_tweet(twitter_stream, i )
        dist_twt  = ssc.sparkContext.parallelize([json_twt], 5)
        rddQueue += [dist_twt]
    lines = ssc.queueStream(rddQueue, oneAtATime=False)
    lines.pprint()


if __name__ == '__main__':
    # create spark configuration
    conf = SparkConf()
    conf.setAppName("TwitterStreamApp")
    # create spark context with the above configuration
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    # create the Streaming Context from the above spark context with interval size 2 seconds
    ssc = StreamingContext(sc, 2)
    # setting a checkpoint to allow RDD recovery
    ssc.checkpoint("checkpoint_TwitterApp")
    twitter_stream = connect_twitter()
    #process_rdd_queue(twitter_stream)


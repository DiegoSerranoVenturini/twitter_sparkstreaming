# coding: utf-8
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
import socket
import json
from config.config import *
import argparse, sys


class TweetListener(StreamListener):
    """Tweet listener socket class. It takes a TCP socket client as input and on events (Twitter tweets)
    will print them on the console and send them to the socket

    Parameters
    ----------
    c_socket: socket object
        TCP socket initialized
    """
    def __init__(self, c_socket):
        self.client_socket = c_socket

    def on_data(self, data):
        """Function to handle incoming data/events

        Parameters
        ----------
        data: events

        Returns
        --------
        return: boolean
            True
        """
        try:
            msg = json.loads(data)
            print(msg['text'].encode('utf-8'))
            self.client_socket.send(msg['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("Error: ", e)
        return True

    def on_error(self, status):
        """Function to handle errors

        Parameters
        ----------
        status:

        Returns
        --------
        return: boolean
            True
        """
        print(status)
        return True


def send_data(c_socket):
    """Main function used to create the tweepy handler and stream. It will initialize the socket and filter tweets

    :param c_socket:
    :return:
    """
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    twitter_stream = Stream(auth, TweetListener(c_socket))
    twitter_stream.filter(track=TRACKS_2_FILTER)


if __name__ == '__main__':  # main routine
    # argument parsing
    parser=argparse.ArgumentParser()
    parser.add_argument('--host', help='socket address', default='127.0.0.1')
    parser.add_argument('--port', help='socket port', default=9999)
    args=parser.parse_args()

    # socket initialization
    s = socket.socket()
    host = args.host
    port = args.port
    s.bind((host, port))
    print('listening on port %i' % port)

    # socket acceptance
    s.listen(5)
    client, addr = s.accept()
    print("Accepted")

    # streaming tweets calling
    send_data(client)


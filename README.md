# twitter_sparkstreaming

This project implements a Spark Streaming tweet processor on pyspark.

## Code Structure

Under `src/main/python` the main scripts can be found.

## Usage

It is necessary to create a folder config with a config.py file with the authentication credentials.

Once that is done run:

`python3 twitter_socket.py --host=HOST --port=PORT`

This command will initialize the Twitter TCP Socket. It will be waiting for the Spark Streaming script to start 
capturing and processing events.

At the time of this commit it only capture and clean the text of the tweets and save it in csv format.

To start capturing tweets run:

`python3 spark_streaming_twitter.py --host=HOST --port=PORT`

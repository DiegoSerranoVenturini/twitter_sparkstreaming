from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, udf, col, size
from pyspark.sql.types import ArrayType, StringType
from utils.text_utils import *
from config.config import *
import argparse, sys

if __name__ == '__main__':
    # argument parsing
    parser=argparse.ArgumentParser()
    parser.add_argument('--host', help='socket address', default='127.0.0.1')
    parser.add_argument('--port', help='socket port', default=9999)
    args=parser.parse_args()

    # spark session initialization
    spark = SparkSession \
        .builder \
        .appName("StructuredToCsv") \
        .getOrCreate()

    # Create DataFrame representing the stream of input lines from connection to ADDRESS:PORT
    lines = spark \
        .readStream \
        .format("socket") \
        .option("host", args.host) \
        .option("port", args.port) \
        .load()

    # udf function declaration (text cleaning + array to string conversion)
    udf_cleantext = udf(cleanup_text , ArrayType(StringType()))
    udf_array_to_string = udf(array_to_string, StringType())

    # Split the lines into processed tokens
    words = lines.select(udf_cleantext(lines.value).alias("text_preprocessed")) \
        .filter(size(col("text_preprocessed"))>0)

    # Start running the query that saves the tweets into the output folder
    query = words \
        .writeStream \
        .outputMode("append") \
        .format("csv") \
        .option('delimiter', '|') \
        .option("checkpointLocation", "checkpoint") \
        .option("path", CSV_STREAMING_OUT_PATH) \
        .start()

    query.awaitTermination()
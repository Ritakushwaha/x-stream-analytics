#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 11 16:53:46 2022

@author: rita
"""

# python kafka_prod_demo.py

from pyspark.sql.types import StructType, StringType, StructField, DateType, ArrayType, MapType
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window, substring
from pyspark.sql.functions import col, from_json
#from bson import BSON
#from bson import json_util
import matplotlib.pyplot as plt

kafka_topic_name = "trump"

kafka_bootstrap_servers = "localhost:9092"

nestTimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"
jsonOptions = {"timestampFormat": nestTimestampFormat}

def all_data(posts_df):
    col_stream = posts_df.writeStream.trigger(processingTime='5 seconds')\
    .outputMode('update')\
    .option("truncate", "false")\
    .option("checkpointLocation", ".checkpoint/col_stream_")\
    .format("console")\
    .start()
                    
    col_stream.awaitTermination(1)


def hashtags_globally(posts_df):

    try:

        hashtags_location = posts_df.select('parsed_value.User_location','parsed_value.Hashtags', substring('timestamp', 1,16).alias("timestamp")) \
            .filter(col("Hashtags") != "[]") \
            .filter(col("Hashtags").contains('Omicron' or 'omicron')) \
            .filter(col("User_location") != "")

        '''
        hashtags_write_location = hashtags_location.writeStream.trigger(processingTime="120 seconds")\
            .outputMode('append')\
            .option("truncate", "false")\
            .option("checkpointLocation", ".checkpoint/h")\
            .format("csv")\
            .option("path","./excel.csv")\
            .start()

        hashtags_write_location.awaitTermination()'''

        hashtags_count_per_location = hashtags_location.groupBy("timestamp","Hashtags").count().select('*')

        hashtags_count_location = hashtags_count_per_location.writeStream.trigger(processingTime='60 seconds')\
            .outputMode('update')\
            .option("truncate", "false")\
            .option("checkpointLocation", "./checkpoint/n")\
            .format("console")\
            .option("failOnDataLoss", "false")\
            .start()

        hashtags_count_location.awaitTermination()
    except Exception as e:
        print(e)


def hashtags_india(hashtags_location):
    '''==============================================Hashtags, Location - India============================================'''
    hashtags_india = hashtags_location \
        .select('Hashtags', 'User_location') \
        .filter(col("User_location").like("%India%"))

    hashtags_india_data = hashtags_india.writeStream.trigger(processingTime='5 seconds')\
        .outputMode('update')\
        .option("truncate", "false")\
        .option("checkpointLocation", "./tree")\
        .format("console")\
        .start()

    hashtags_india_data.awaitTermination()


if __name__ == "__main__":

    sparkSession = SparkSession \
        .builder \
        .appName("TwitterStreamingAssignment") \
        .master("local") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    schema = StructType() \
        .add("Created_date", DateType()) \
        .add("Username", StringType()) \
        .add("User_location", StringType())\
        .add("Hashtags", StringType()) \
        .add("Text", StringType())

    posts_df = sparkSession.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "trump") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()\
        .select(from_json(col("value").cast("string"), schema, jsonOptions).alias("parsed_value"), col('timestamp'))

    posts_df.printSchema()


    # all data

    #all_data(posts_df)

    hashtags_globally(posts_df)

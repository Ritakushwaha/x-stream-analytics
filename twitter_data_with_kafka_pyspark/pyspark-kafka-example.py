#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 11 16:53:46 2022

@author: rita
"""

from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window

kafka_topic_name = "trump"

kafka_bootstrap_servers = "localhost:9092"

if __name__ == "__main__":
    

    sparkSession = SparkSession \
        .builder \
        .appName("TwitterStreamingAssignment") \
        .master("local") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .getOrCreate()

    # id,status_update,created_by,created_time,media_url,posted_ip,privacy_settings,post_status
    post_df = sparkSession.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "trump") \
        .option("startingOffsets", "earliest") \
        .load()
                    
    posts_df.printSchema()
    posts_df1 = posts_df.selectExpr("CAST(value as STRING)", "timestamp")
    posts_schema = StructType() \
        .add("id", StringType())\
            .add("status_update", StringType())\
                .add("created_by", StringType())\
                    .add("created_time", StringType())\
                        .add("media_url", StringType())\
                            .add("posted_ip", StringType())\
                                .add("privacy_settings", StringType())\
                                    .add("post_status", StringType())

    posts_df2 = posts_df1.select(from_json(col("value"), posts_schema)\
        .alias("posts"), "timestamp")
    # posts_df2.printSchema()
    posts_df3 = posts_df2.where(posts_df2['posts.post_status']=="active").select("posts.*", "timestamp")
    posts_df3 = posts_df2.select("posts.*", "timestamp")
    # posts_df3.writeStream.outputMode("append").format('console').start()
    posts_df4 = posts_df3.groupBy("created_by")\
        .agg({"created_by":"count"}).alias("posts")
    posts_df5 = posts_df3.groupBy("post_status").agg({"post_status":"count"}).alias("status_count")
    # posts_df4.printSchema()
    posts_time = posts_df3.groupBy(year('created_time')).agg({"id":"count"}).alias("year_count")

    posts_stream = posts_df4.writeStream.trigger(processingTime='5 seconds')\
        .outputMode('update')\
            .option("truncate", "false")\
                .format("console")\
                    .start()
    post_status_stream =posts_df5.writeStream.trigger(processingTime='5 seconds')\
        .outputMode('update')\
            .option("truncate", "false")\
                .format("console")\
                    .start()
    post_time_stream =posts_time.writeStream.trigger(processingTime='5 seconds')\
        .outputMode('complete')\
            .option("truncate", "false")\
                .format("console")\
                    .start()
    posts_stream.awaitTermination()
    post_status_stream.awaitTermination()
    post_time_stream.awaitTermination()
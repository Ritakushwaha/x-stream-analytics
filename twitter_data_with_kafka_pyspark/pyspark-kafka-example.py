#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 11 16:53:46 2022

@author: rita
"""


from pyspark.sql.types import StructType, StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window
from pyspark.sql.functions import *

kafka_topic_name = "trump"

kafka_bootstrap_servers = "localhost:9092"

if __name__ == "__main__":
    sparkSession = SparkSession \
        .builder \
        .appName("TwitterStreamingAssignment") \
        .master("local") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .getOrCreate()

    posts_df = sparkSession.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "trump") \
        .option("startingOffsets", "earliest") \
        .load()
                    
    posts_df1 = posts_df.selectExpr("CAST(value as STRING)", "timestamp")
    posts_df1.printSchema()
    
    columns = posts_df1\
        .select(explode(split(posts_df1.value, "\n")).alias("columns"),posts_df1["timestamp"])        
    columns.printSchema()
    
    col_value = columns.select(explode(split(columns, ":")).alias("values"),columns["timestamp"])
    col_value.printSchema()
    
    posts_schema = StructType() \
        .add("created_date", StringType())\
            .add("user_name", StringType())\
                .add("user_location", StringType())\
                    .add("hashtags", StringType())\
                        .add("text", StringType())
    
    posts_df2 = posts_df1.select(from_json(col("value"), posts_schema)\
        .alias("posts"), "timestamp")
    #posts_df2.printSchema()


    '''posts_stream = posts_df2.writeStream.trigger(processingTime='5 seconds')\
    .outputMode('append')\
        .option("checkpointLocation", "./checkpoint")\
        .option("truncate", "false")\
            .format("csv")\
                .option("path","./twitter_data.csv")\
                    .start()'''
                    
                    
    posts_stream = posts_df2.writeStream.trigger(processingTime='5 seconds')\
    .outputMode('append')\
        .option("truncate", "false")\
            .format("console")\
                    .start()

    '''    
    
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
                    '''
    posts_stream.awaitTermination()
    post_status_stream.awaitTermination()
    post_time_stream.awaitTermination()
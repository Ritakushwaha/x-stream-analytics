#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  7 14:04:48 2022

@author: rita
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window

sparkSession = SparkSession \
    .builder \
    .appName("TwitterStreamingAssignment") \
    .master("local") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .getOrCreate()

twitterDataDF = sparkSession.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "trump") \
    .option("startingOffsets", "earliest") \
    .load()
    
twitterDataDF.writeStream.outputMode("update").format("console").start()

#twitterDataDF = twitterDataDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")

#print(twitterDataDF)

'''
words = twitterDataDF\
    .select(explode(split(twitterDataDF.value, " ")).alias("word"),twitterDataDF["timestamp"])

count = words.select(words['word'],words['timestamp'])\
    .filter(words["word"] == "omicron")\
    .groupBy(window(words["timestamp"],"2 minutes"),words["word"])\
    .count()


query = count.writeStream.outputMode("complete").format("console").start()

sparkSession.streams.awaitAnyTermination()
'''

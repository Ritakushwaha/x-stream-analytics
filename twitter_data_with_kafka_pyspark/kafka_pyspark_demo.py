#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  7 14:04:48 2022

@author: rita
"""
import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == '__main__':
    sc = SparkContext(appName='Kafka_Spark')
    ssc = StreamingContext(sc, 60)
    
    message = KafkaUtils.createDirectStream(ssc, topics='trump',kafkaParams={'metadata.broker.list':'localhost:9092'})
    
    words = message.map(lambda x: x[1]).flatmap(lambda x:x.split(" "))
    wordcount = words.map(lambda x: (x,1)).reduceByKey(lambda a,b :a+b)
    
    wordcount.pprint()
    
    ssc.start()
    ssc.awaitTermination()
    
    

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  6 12:14:34 2022

@author: rita
"""
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
import pandas as pd
import time

d ={}
l = []
consumer = KafkaConsumer('trump')
#consumer = KafkaConsumer('trump',value_deserializer=lambda m: literal_eval(m.decode('utf8')))

def consumer_reader():
    for message in consumer:
        for i in message.value.decode('utf8').split('\n'):
            l.append(i.split(':'))
        for j in l:
            if len(j)>1:
                d[j[0]] = [j[1]]
        df = pd.DataFrame(d)
        #df['Hashtags'].to_csv('./output.csv',mode='a',header=False,index=False)
        
        count =0
        omi_df = df
        #print(df['Hashtags'][0])
        #print(len(df['Hashtags'][0]))
        
        if len(df['Hashtags'][0])>2 :
            if 'Omicron' in df['Hashtags'][0] or 'omicron' in df['Hashtags'][0]:
                omi_df = df['Hashtags'][0]
            else:
                print('not found')
        print(omi_df)
        


def periodic_work(interval):
    while True:
        print(consumer_reader())
        # interval should be an integer, the number of seconds to wait
        time.sleep(interval)
        
periodic_work(60*0.1)


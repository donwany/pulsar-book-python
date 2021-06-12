#!/usr/bin/python

from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka import TopicPartition
from datetime import datetime
from random import gauss
from time import sleep
import pandas as pd
import numpy as np
import sys
import json
from json import loads
from ast import literal_eval
from pymongo import MongoClient
from pandas.io.json import json_normalize


topic = 'measurement'
server = "127.0.0.1:9092"
client = MongoClient('127.0.0.1:27017')
collection = client['measurement']


def main():

	consumer = KafkaConsumer(topic, 
		bootstrap_servers=server, 
		auto_offset_reset='earliest',
		enable_auto_commit=True,
		value_deserializer=lambda x: loads(x.decode('utf-8')))

	for msg in consumer:

		message = literal_eval(msg.value.decode('utf-8'))
		collection.insert_one(message)

    	print('{} added to {}'.format(message, collection))

if __name__ == "__main__":
    main()


from pandas.io.json import json_normalize
from sklearn.linear_model import LogisticRegression
from kafka import KafkaProducer
from kafka import KafkaConsumer
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import udf
from datetime import datetime
from ast import literal_eval
from pymongo import MongoClient
from time import sleep
from json import loads
import pandas as pd
import numpy as np
import json
import sys
import os
import dns
import ast
from bson import BSON
from bson.son import SON



#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0'

client = MongoClient("mongodb+srv://mobilemoney:Abc12345@mobilemoney-q3w48.mongodb.net/LogitModelPrediction?retryWrites=true&w=majority")
db = client["LogitModelPrediction"]
collection = db["ModelPrediction"]

topic = "dsp"
server = ['127.0.0.1:9092']


# build logistic regression model
gamesDF = pd.read_csv("https://github.com/bgweber/Twitch/raw/master/Recommendations/games-expand.csv")

model = LogisticRegression()
model.fit(gamesDF.iloc[:,0:10], gamesDF['label'])

# define the UDF for scoring users
def score(row):
	d = json.loads(row)
	p = pd.DataFrame.from_dict(d, orient = "index").transpose()
	pred = model.predict_proba(p.iloc[:,0:10])[0][0]
	result = {"User_ID":d['User_ID'],
			 "G1": d['G1'], 
			 "G2": d['G2'], 
			 "G3": d['G3'], 
			 "G4": d['G4'], 
			 "G5": d['G5'], 
			 "G6": d['G6'], 
			 "G7": d['G7'], 
			 "G8": d['G8'], 
			 "G9": d['G9'], 
			 "G10":d['G10'], 
			 'pred': pred}
	return str(json.dumps(result))


def main():

	consumer = KafkaConsumer(topic,
		bootstrap_servers = server,
		value_deserializer=lambda m: json.loads(m.decode('ascii')))

	producer = KafkaProducer(bootstrap_servers=server, 
							value_serializer=lambda m: json.dumps(m).encode('ascii'))

	try:
		# while True:
		for msg in consumer:
			#message = msg.value
			record = json.dumps(msg.value)

			results = score(record)

			# send to predict topic
			producer.send("pred", results)

			#record['pred'] = results['pred']
			# insert into mongodb collection
			collection.insert_one(ast.literal_eval(results))
			print('{} added to {}'.format(results, collection))

		time.sleep(5)

	except Exception as e:
		raise e
	

if __name__ == "__main__":
    main()





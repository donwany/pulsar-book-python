from kafka import KafkaProducer
from kafka import KafkaConsumer
from datetime import datetime
from time import sleep
import pandas as pd
import numpy as np
import sys
import json
import time
import uuid
from json import dumps
from json import loads
from ast import literal_eval
from pandas.io.json import json_normalize


topic = "dsp"
server = ['127.0.0.1:9092']


def gen():
	s = np.random.uniform(0,1)
	if s > 0.5:
		return 1
	else:
		return 0



def main():

	producer = KafkaProducer(bootstrap_servers=server, 
							value_serializer=lambda m: json.dumps(m).encode('ascii'))
	#print ("*** Starting kafka stream on " + str(server) + ", topic : " + topic)

	try:
		while True:
			data = {"G1":gen(), 
					"G2":gen(),
					"G3":0,
					"G4":gen(),
					"G5":1,
					"G6":gen(),
					"G7":0,
					"G8":gen(),
					"G9":0,
					"G10":1,
					"User_ID": str(uuid.uuid1())}
			# send data
			producer.send(topic, data)
			print('Message published successfully to topic -{}'.format(topic))

			time.sleep(5)

	except Exception as e:
		print("\nIntercepted user interruption ..\nBlock until all pending messages are sent..")
		producer.flush()
	
if __name__ == "__main__":
    main()





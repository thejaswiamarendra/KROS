from kafka import KafkaConsumer
import time
import json
import sys
from statistics import mean
import pandas as pd
import logging

class latencySub:
	def __init__(self, topic, conf):
		
		self.consumer = KafkaConsumer(topic,
                                 bootstrap_servers=conf['bootstrap.servers'],
                                 sasl_plain_username=conf['sasl.username'],
                                 sasl_plain_password=conf['sasl.password'],
                                 security_protocol=conf['security.protocol'],
                                 sasl_mechanism=conf['sasl.mechanism'],
                                 value_deserializer=conf['value.deserializer'])
		self.valueList = []
		self.averages = {}
				
	def consume(self):
		for msg in self.consumer:
			if json.loads(msg.value)=="QUIT":
				print(self.averages)
				df = pd.DataFrame(columns = ["bytes", "time"])
				df["bytes"] = self.averages.keys()
				df["time"] = self.averages.values()
				df.to_csv("kafkavalues.csv")
				sys.exit()
			else:
				if json.loads(msg.value)["change"]:
					self.averages[sys.getsizeof(json.loads(msg.value)["message"])] = mean(self.valueList)
					self.valueList = []
					print(self.averages)
				else:
					self.valueList.append(int(time.time()*1000) - msg.timestamp)
					print(int(time.time()*1000) - msg.timestamp)

if __name__ == "__main__":

        conf = {
            'bootstrap.servers': 'pkc-41p56.asia-south1.gcp.confluent.cloud:9092',
            'sasl.username': 'J2LXJ2BZT5ZA63EE',
            'sasl.password': 'qrnWDawOdsWxJ/l2i/LOX6yFsX+RFS9Hs2EYQHi/p0wcTVfxlX2FxlmLPBKdABTX',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'value.deserializer':lambda x: x.decode('utf-8')
        }
	
	sub = latencySub('topic0', conf)
	sub.consume()

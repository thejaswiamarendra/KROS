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
				df.to_csv("values.csv")
				sys.exit()
			else:
				if json.loads(msg.value)["change"]:
					self.averages[json.loads(msg.value)["size"]] = mean(self.valueList)
					self.valueList = []
					print(self.averages)
				else:
					self.valueList.append(int(time.time()*1000) - msg.timestamp)
					print(int(time.time()*1000) - msg.timestamp)

if __name__ == "__main__":
                conf = {
                    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
                    'sasl.username': 'PCWTDO5Z3CALZTNC',
                    'sasl.password': 'jEHF9KmZl4rJnXe1dyl+Hi+6v6FmM5V3JZbP5acSYV9DUxowBe+LcYU2Ks4RUR9B',
                    'security.protocol': 'SASL_SSL',
                    'sasl.mechanism': 'PLAIN',
                    'value.deserializer':lambda x: x.decode('utf-8')
                }
                sub = latencySub('cloud_4', conf)
                sub.consume()

from kafka import KafkaProducer
import time
import json
import logging
import base64
import sys

def serializer(message):
    print(sys.getsizeof(json.dumps(message).encode("utf-8")))
    return json.dumps(message).encode("utf-8")
    
    
	
	
class latencyPub:
	def __init__(self, testCases, topic, conf):
		self.producer = KafkaProducer(bootstrap_servers=conf['bootstrap.servers'],
                         sasl_plain_username=conf['sasl.username'],
                         sasl_plain_password=conf['sasl.password'],
                         security_protocol=conf['security.protocol'],
                         sasl_mechanism=conf['sasl.mechanism'],
                         value_serializer=conf['value.serializer'])
		self.body = 'X'
		self.topic = topic
		self.testCases = testCases
		self.message = {"change":False, "body":None,"size":None}
	
	def publish(self):
		for i in self.testCases:
			self.message["body"] = (self.body*i)
			self.message["change"] = False
			self.message["size"]=i
			j = 0
			while j <100:
				self.producer.send(self.topic, self.message)
				#self.producer.send('cloud_2', self.message)
				#self.producer.send('cloud_3', self.message)
				#self.producer.send('cloud_4', self.message)
				#self.producer.send('cloud_5', self.message)
				#self.producer.send('cloud_6', self.message)
				#self.producer.send('cloud_7', self.message)
				#self.producer.send('cloud_8', self.message)
				print(self.message)
				#print(sys.getsizeof(json.dumps(self.message).encode("utf-8")))
				j+=1
			self.message["change"] = True
			self.producer.send(self.topic, self.message)
			#self.producer.send('cloud_2', self.message)
			#self.producer.send('cloud_3', self.message)
			#self.producer.send('cloud_4', self.message)
			#self.producer.send('cloud_5', self.message)
			#self.producer.send('cloud_6', self.message)
			#self.producer.send('cloud_7', self.message)
			#self.producer.send('cloud_8', self.message)
			print(self.message)
			time.sleep(1)
		
		while 1:
			self.producer.send(self.topic, "QUIT")
			#self.producer.send('cloud_2', "QUIT")
			#self.producer.send('cloud_3', "QUIT")
			#self.producer.send('cloud_4', "QUIT")
			#self.producer.send('cloud_5', "QUIT")
			#self.producer.send('cloud_6', "QUIT")
			#self.producer.send('cloud_7', "QUIT")
			#self.producer.send('cloud_8', "QUIT")
			print("QUIT")
			
if __name__ == "__main__":
                conf = {
                    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
                    'sasl.username': 'PCWTDO5Z3CALZTNC',
                    'sasl.password': 'jEHF9KmZl4rJnXe1dyl+Hi+6v6FmM5V3JZbP5acSYV9DUxowBe+LcYU2Ks4RUR9B',
                    'security.protocol': 'SASL_SSL',
                    'sasl.mechanism': 'PLAIN',
                    'value.serializer':lambda x: json.dumps(x).encode('utf-8')
                }
                topic = 'cloud_1'
                pub = latencyPub(list(range(1,100000,5000)), topic, conf)
                pub.publish()

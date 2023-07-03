from kafka import KafkaProducer
import time
import json
import logging

def serializer(message):
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
		self.message = {"change":False, "body":None}
	
	def publish(self):
		for i in self.testCases:
			self.message["body"] = self.body*i
			self.message["change"] = False
			j = 0
			while j <100:
				self.producer.send(self.topic, self.message)
				print(self.message)
				j+=1
			self.message["change"] = True
			self.producer.send(self.topic, self.message)
			print(self.message)
			time.sleep(1)
		
		while 1:
			self.producer.send(self.topic, "QUIT")
			print("QUIT")
			
if __name__ == "__main__":
        conf = {
            'bootstrap.servers': 'pkc-41p56.asia-south1.gcp.confluent.cloud:9092',
            'sasl.username': 'J2LXJ2BZT5ZA63EE',
            'sasl.password': 'qrnWDawOdsWxJ/l2i/LOX6yFsX+RFS9Hs2EYQHi/p0wcTVfxlX2FxlmLPBKdABTX',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'value.serializer':lambda x: json.dumps(x).encode('utf-8')
        }

        topic = 'topic0'
	pub = latencyPub(list(range(1, 250000, 10000)), topic, conf)
	pub.publish()

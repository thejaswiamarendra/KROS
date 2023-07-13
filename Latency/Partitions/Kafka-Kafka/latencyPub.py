from kafka import KafkaProducer
import time
import json
 
def serializer(message):
    return json.dumps(message).encode("utf-8")
    
    
	
	
class latencyPub:
	def __init__(self, servers, testCases, topic):
		self.producer = KafkaProducer(
				    bootstrap_servers=servers,
				    api_version=(0,10,1),
				    value_serializer=serializer
				)
		self.body = 'X'
		self.topic = topic
		self.testCases = testCases
		self.message = {"change":False, "body":None}
	
	def publish(self):
		for i in self.testCases:
			self.message["body"] = self.body*i
			self.message["change"] = False
			j = 0
			while j <1000:
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
	pub = latencyPub(["localhost:9092"], list(range(1, 1000, 100)), "test")
	pub.publish()
			

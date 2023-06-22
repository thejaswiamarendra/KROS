from kafka import KafkaConsumer
import time
import json
import sys
from statistics import mean
import pandas as pd
class latencySub:
	def __init__(self, topic, servers):
		
		self.consumer = KafkaConsumer(
				topic,
				bootstrap_servers = servers,
				auto_offset_reset = "latest")
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
					self.averages[sys.getsizeof(json.loads(msg.value)["body"])] = mean(self.valueList)
					self.valueList = []
					print(self.averages)
				else:
					self.valueList.append(int(time.time()*1000) - msg.timestamp)
					print(int(time.time()*1000) - msg.timestamp)

if __name__ == "__main__":

#	i = 0
#	consumer = KafkaConsumer(
#	"test",
#	bootstrap_servers = "localhost:9092",
#	auto_offset_reset = "latest")
#	curr = 0
#	for msg in consumer:
#		if i>100000:
#			break
#		print(sys.getsizeof(msg.value))
#		
#		print(int(time.time()*1000), msg.timestamp)
#		curr+=-msg.timestamp + int(time.time()*1000)
#		i+=1
#	print("Average = ", curr/100000)
	
	sub = latencySub("test", "localhost:9092")
	sub.consume()

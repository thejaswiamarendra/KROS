import rospy
from kafka import KafkaProducer, KafkaConsumer
from std_msgs.msg import String
import time
import json
import os
import threading

class KROSbridge:
	nodeNames = [0]
	def __init__(self, topic, cloud_conf, local_topic, datatype):
		self.cloud_conf = cloud_conf
		self.topic = topic
		self.local_topic = local_topic
		self.datatype = datatype
		try:
			self.cloudPublisher = KafkaProducer(bootstrap_servers=self.cloud_conf['bootstrap.servers'],
				         sasl_plain_username=self.cloud_conf['sasl.username'],
				         sasl_plain_password=self.cloud_conf['sasl.password'],
				         security_protocol=self.cloud_conf['security.protocol'],
				         sasl_mechanism=self.cloud_conf['sasl.mechanism'],
				         value_serializer=self.cloud_conf['value.serializer'])
		except Exception as e:
			print("Error while creating cloud Publisher ", e)
		self.nodeName = "KROSpub" + str(KROSbridge.nodeNames[-1])
		KROSbridge.nodeNames.append(KROSbridge.nodeNames[-1]+1)
		rospy.init_node(self.nodeName, anonymous = True, log_level=rospy.INFO, disable_signals=False)
			
		
	def callback(self, message):
		self.cloudPublisher.send(self.topic, message)
	
	def run(self):
		rospy.Subscriber(self.local_topic, self.datatype, self.callback)
		
		rospy.spin()
			

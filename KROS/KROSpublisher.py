import rospy
from kafka import KafkaProducer, KafkaConsumer
from std_msgs.msg import String
import time
import json
import os
import threading


'''
cloud_conf = {
    'bootstrap.servers': 'pkc-41p56.asia-south1.gcp.confluent.cloud:9092',
    'sasl.username': 'J2LXJ2BZT5ZA63EE',
    'sasl.password': 'qrnWDawOdsWxJ/l2i/LOX6yFsX+RFS9Hs2EYQHi/p0wcTVfxlX2FxlmLPBKdABTX',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'value.serializer':lambda x: json.dumps(x).encode('utf-8')
        }
'''

'''
LAN_conf = {
	'ROS_MASTER_URI' = https://localhost:11311,
	'ROS_HOSTNAME' = localhost
	}
'''

class KROSpublisher:
	'''
		KROSpublisher - Publishes messages on a topic locally, over a network and over cloud. 
		Messages are published locally and over a network using ROS, but over cloud using Kafka.
		Default message format is String.
	'''
	
	
	'''
		nodeNames -> Static list of all the KROS node names. The last integer in this list is added to the names of the node
	'''
	nodeNames = [0] 
	
	def __init__(self, send_local = False, cloud_conf = None, topic = None, local_queue_size = 10, msg_class = String):
							
							
		'''					
		try:
			rospy.get_master().getPid()
		except:
			kafka_thread = threading.Thread(target=self.startRoscore())
			kafka_thread.start()
		'''

		self.topic = topic
		self.local_queue_size = local_queue_size
		self.msg_class = msg_class
		self.cloud_conf = cloud_conf
		self.send_local = send_local
		
		'''
			Only create a Kafka-Cloud Publisher if cloud_conf is not None. The format for the cloud_conf is given earlier in this file.
		'''
		if cloud_conf is None:
			self.cloudPublisher = None
			
				        
		else:
			try:
				self.cloudPublisher = KafkaProducer(bootstrap_servers=self.cloud_conf['bootstrap.servers'],
				         sasl_plain_username=self.cloud_conf['sasl.username'],
				         sasl_plain_password=self.cloud_conf['sasl.password'],
				         security_protocol=self.cloud_conf['security.protocol'],
				         sasl_mechanism=self.cloud_conf['sasl.mechanism'],
				         value_serializer=self.cloud_conf['value.serializer'])
			except Exception as e:
				print("Error while creating cloud Publisher ", e)
			
		self.nodeName = "KROSpub" + str(KROSpublisher.nodeNames[-1])
		KROSpublisher.nodeNames.append(KROSpublisher.nodeNames[-1]+1)
		if self.send_local == True:
			try:
				self.localPublisher = rospy.Publisher(self.topic, self.msg_class, queue_size = self.local_queue_size)
			except Exception as e:
				print("Error while creating local publisher ", e)
			rospy.init_node(self.nodeName, anonymous = True, log_level=rospy.INFO, disable_signals=False)
		else:
			self.localPublisher = None
	
		
		'''
			Create a unique nodeName using the static list
		'''
		
		
		
		
		
		
	def publish(self, message):
		'''
			If there is no cloud Publisher just publish the values locally else publish both locally and over cloud.
		'''
		if self.cloudPublisher is None:
			try:
				rospy.loginfo(message)
				self.localPublisher.publish(message)
			except Exception as e:
				print("Error while publishing locally ", e)
			
			
		elif self.localPublisher is None:
			
			try: 
				self.cloudPublisher.send(self.topic, message)
			except Exception as e:
				print("Error while publishing over cloud ", e)
		else:
			try:
				rospy.loginfo(message)
				self.localPublisher.publish(message)
			except Exception as e:
				print("Error while publishing locally ", e)
				
			try: 
				self.cloudPublisher.send(self.topic, message)
			except Exception as e:
				print("Error while publishing over cloud ", e)
	
	
	def set_topic(self, topic):
		self.topic = topic
	
	def get_topic(self):
		return self.topic
		
	def set_cloud_conf(self, cloud_conf):
		self.cloud_conf = cloud_conf
		
	def get_cloud_conf(self):
		return self.cloud_conf

			
			
			

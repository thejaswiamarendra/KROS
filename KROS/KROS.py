import rospy
from kafka improt KafkaProducer, KafkaConsumer
from std_msgs.msg import String
import time
import json
import os
from Roscore import Roscore	



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
	
	def __init__(self, cloud_conf = None, LAN_conf = {'ROS_MASTER_URI' = https://localhost:11311,
							'ROS_HOSTNAME' = localhost}, topic = None, local_queue_size = 10, msg_class = String):
		self.topic = topic
		self.local_queue_size = local_queue_size
		self.msg_class = msg_class
		'''
			Only create a Kafka-Cloud Publisher if cloud_conf is not None. The format for the cloud_conf is given earlier in this file.
		'''
		if !cloud_conf:
			try:
				self.cloudPublisher = KafkaProducer(bootstrap_servers=conf['bootstrap.servers'],
				         sasl_plain_username=cloud_conf['sasl.username'],
				         sasl_plain_password=cloud_conf['sasl.password'],
				         security_protocol=cloud_conf['security.protocol'],
				         sasl_mechanism=cloud_conf['sasl.mechanism'],
				         value_serializer=cloud_conf['value.serializer'])
			except Exception as e:
				print("Error while creating cloud Publisher ", e)
				        
		else:
			self.cloudPublisher = None
			
			
			
		'''
			LAN_conf is to configure the ROS master node IP address. By default it is set to localhost, but can be changed accordingly.
		'''
		
		os.environ['ROS_MASTER_URI'] = LAN_conf['ROS_MASTER_URI']
		os.environ['ROS_HOSTNAME'] = LAN_conf['ROS_HOSTNAME']
		
		try:
			self.localPublisher = rospy.Publisher(self.topic, msg_class = self.msg_class, queue_size = self.local_queue_size)
		except Exception as e:
			print("Error while creating local publisher ", e)
	
		
		'''
			Create a unique nodeName using the static list
		'''
		self.nodeName = "KROSpub" + str(nodeNames[-1])
		nodeNames.append(self.nodeNames[-1]+1)
		rospy.init_node(self.nodeName, anonymous = True, log_level=rospy.INFO, disable_signals=False)
		
		
		
		'''
			Roscore is a singleton class.
		'''
		if(!Roscore.__initialised):
			self.startRoscore()
		
	def publish(self, message):
		'''
			If there is no cloud Publisher just publish the values locally else publish both locally and over cloud.
		'''
		if !self.cloudPublisher:
			try:
				rospy.loginfo(message)
				self.localPublisher.publish(message)
			except Exception as e:
				print("Error while publishing locally ", e)
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
	
	def startRoscore(self):
		roscore = Roscore()
		roscore.run()
	
	def terminateRoscore(self):
		roscore.terminate()	
	
	def set_topic(self, topic):
		self.topic = topic
	
	def get_topic(self):
		return self.topic
	
	def set_LAN_conf(self, LAN_conf):
		self.LAN_conf = LAN_conf
	
	def get_LAN_conf(self):
		return self.LAN_conf
		
	def set_cloud_conf(self, cloud_conf):
		self.cloud_conf = cloud_conf
		
	def get_cloud_conf(self):
		return self.cloud_conf
	
			
			
		

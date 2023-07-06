import rospy
from kafka improt KafkaProducer, KafkaConsumer
from std_msgs.msg import String
import time
import json
import os
from Roscore import Roscore	
import threading


'''
cloud_conf = {
            'bootstrap.servers': 'pkc-41p56.asia-south1.gcp.confluent.cloud:9092',
            'sasl.username': 'J2LXJ2BZT5ZA63EE',
            'sasl.password': 'qrnWDawOdsWxJ/l2i/LOX6yFsX+RFS9Hs2EYQHi/p0wcTVfxlX2FxlmLPBKdABTX',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'value.deserializer':lambda x: x.decode('utf-8')
        }
'''

'''
LAN_conf = {
	'ROS_MASTER_URI' = https://localhost:11311,
	'ROS_HOSTNAME' = localhost
	}
'''

class KROSsubscriber:
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
							'ROS_HOSTNAME' = localhost}, topic = None, callback = None, msg_class = String):
		self.topic = topic
		self.msg_class = msg_class
		self.callback = local_callback
		self.cloud_conf = cloud_conf
		'''
			Only create a Kafka-Cloud Publisher if cloud_conf is not None. The format for the cloud_conf is given earlier in this file.
		'''
		if !cloud_conf:
			try:
				self.cloudSubscriber = KafkaConsumer(self.topic,
				                 bootstrap_servers=self.cloud_conf['bootstrap.servers'],
				                 sasl_plain_username=self.cloud_conf['sasl.username'],
				                 sasl_plain_password=self.cloud_conf['sasl.password'],
				                 security_protocol=self.cloud_conf['security.protocol'],
				                 sasl_mechanism=self.cloud_conf['sasl.mechanism'],
				                 value_deserializer=self.cloud_conf['value.deserializer'])
			except Exception as e:
				print("Error while creating Cloud Subscriber ", e)
				        
		else:
			self.cloudSubscriber = None
			
			
			
		'''
			LAN_conf is to configure the ROS master node IP address. By default it is set to localhost, but can be changed accordingly.
		'''
		
		os.environ['ROS_MASTER_URI'] = LAN_conf['ROS_MASTER_URI']
		os.environ['ROS_HOSTNAME'] = LAN_conf['ROS_HOSTNAME']
	
		
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
		
	def subscribe(self):
		rospy.Subscriber(self.topic, String, self.callback)
		kafka_thread = threading.Thread(target=self.subscriber_cloud)
		kafka_thread.start()
		rospy.spin()
	
	def subscriber_cloud(self):
		try:
			for message in consumer:
				if message is None:
					continue

				if message.error():
		# Error occurred while consuming message
					error = message.error()
				if error.code() == KafkaError._PARTITION_EOF:
		# End of partition, continue consuming
					continue
				else:
		# Handle other Kafka errors
					print('Error: {}'.format(error))
					break

		# Process Kafka message here
			if(callback is None):
				print('Received message: {}'.format(message.value.decode('utf-8')))
			else:
				self.callback(message)

		except KeyboardInterrupt:
		# User interrupted the consumer
			print('Consumer interrupted')

		finally:
		# Close Kafka consumer
			consumer.close()
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

			
			
		

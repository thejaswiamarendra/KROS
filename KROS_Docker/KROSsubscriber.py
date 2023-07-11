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
	
	def __init__(self, cloud_conf = None, topic = None, local_callback = None, cloud_callback = None, msg_class = String):
		self.topic = topic
		self.msg_class = msg_class
		self.local_callback = local_callback
		self.cloud_callback = cloud_callback
		self.cloud_conf = cloud_conf
		'''
			Only create a Kafka-Cloud Publisher if cloud_conf is not None. The format for the cloud_conf is given earlier in this file.
		'''
		if cloud_conf is not None:
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
		
	
		
		'''
			Create a unique nodeName using the static list
		'''
		self.nodeName = "KROSpub" + str(KROSsubscriber.nodeNames[-1])
		KROSsubscriber.nodeNames.append(KROSsubscriber.nodeNames[-1]+1)
		if self.local_callback is not None:
			rospy.init_node(self.nodeName, anonymous = True, log_level=rospy.INFO, disable_signals=False)
		
		
		
		'''
			Roscore is a singleton class.
		'''
		
		
	def subscribe(self):
		if self.local_callback is None:
			try:
				kafka_thread = threading.Thread(target=self.subscriber_cloud)
				kafka_thread.start()
			except KeyboardInterrupt as e:
				exit(0)
		elif self.cloudSubscriber is None:
			try:
				rospy.Subscriber(self.topic, String, self.local_callback)
				rospy.spin()
			except KeyboardInterrupt as e:
				exit(0)
		else:
			try:
				rospy.Subscriber(self.topic, String, self.local_callback)
				kafka_thread = threading.Thread(target=self.subscriber_cloud)
				kafka_thread.start()
				rospy.spin()
			except KeyboardInterrupt as e:
				exit(0)
			
	
	def subscriber_cloud(self):
		try:
			for message in self.cloudSubscriber:

				if message is None:
					continue
				'''
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
				'''
		# Process Kafka message here
				if(self.cloud_callback is None):
					print('Received message: {}'.format(message.value.decode('utf-8')))
				else:
					self.cloud_callback(message)

		except KeyboardInterrupt:
		# User interrupted the consumer
			print('Consumer interrupted')

		finally:
		# Close Kafka consumer
			self.cloudSubscriber.close()

	def set_topic(self, topic):
		self.topic = topic
	
	def get_topic(self):
		return self.topic
	
		
	def set_cloud_conf(self, cloud_conf):
		self.cloud_conf = cloud_conf
		
	def get_cloud_conf(self):
		return self.cloud_conf

			
			
		

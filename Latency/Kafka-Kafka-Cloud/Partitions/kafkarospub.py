import rospy
from std_msgs.msg import String
import time
import json
from kafka import KafkaProducer
	
		
class KROSPub:
	def __init__(self, messageSize, nodeName, topic, testCases, KafkaPub, KafkaTopic):
		self.message_publisher = rospy.Publisher(topic, String, queue_size=10)
		self.body = "X"
		self.message = {"time":None, "message":None, "change":False}
		self.testCases = testCases
		rospy.init_node(nodeName, anonymous = False)
		self.KafkaPub = KafkaPub
	        self.Kafkatopic = KafkaTopic
	        
	        
	def messagePublisher(self):
		for i in self.testCases:
			self.message["change"] = False
			self.message["message"] = self.body*i
			j = 0
			while j<100:
				self.message["time"] = time.time()
				rospy.loginfo(self.message)
				self.message_publisher.publish(json.dumps(self.message))
				self.KafkaPub.send(self.Kafkatopic, self.message)
				j+=1
			self.message["change"] = True
			self.message_publisher.publish(json.dumps(self.message))
			self.KafkaPub.send(self.Kafkatopic, self.message)
			time.sleep(1)
		self.message = {}
		while not rospy.is_shutdown():
			print("QUIT")
			rospy.loginfo(self.message)
			self.message_publisher.publish(json.dumps(self.message))
			self.KafkaPub.send(self.Kafkatopic, "QUIT")
		
def serializer(message):
    return json.dumps(message).encode("utf-8")
    


if __name__ == "__main__":
        conf = {
            'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
            'sasl.username': 'PCWTDO5Z3CALZTNC',
            'sasl.password': 'jEHF9KmZl4rJnXe1dyl+Hi+6v6FmM5V3JZbP5acSYV9DUxowBe+LcYU2Ks4RUR9B',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'value.serializer':lambda x: json.dumps(x).encode('utf-8')
        }
        
        producer = KafkaProducer(bootstrap_servers=conf['bootstrap.servers'],
                         sasl_plain_username=conf['sasl.username'],
                         sasl_plain_password=conf['sasl.password'],
                         security_protocol=conf['security.protocol'],
                         sasl_mechanism=conf['sasl.mechanism'],
                         value_serializer=conf['value.serializer'])
        
	krospub = KROSPub(1, 'PubNode', 'messageTopic', list(range(1, 100000, 5000)), producer, 'topic0')
	time.sleep(5)
	try:
		krospub.messagePublisher()
	except rospy.ROSInterruptException:
		pass	
			

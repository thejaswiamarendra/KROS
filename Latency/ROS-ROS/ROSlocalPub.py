import rospy
from std_msgs.msg import String
import time
import json
	
		
class LatencyPub:
	def __init__(self, messageSize, nodeName, topic, testCases):
		self.message_publisher = rospy.Publisher(topic, String, queue_size=10)
		self.body = "X"
		self.message = {"time":None, "message":None, "change":False}
		self.testCases = testCases
		rospy.init_node(nodeName, anonymous = True)
	
	def messagePublisher(self):
		for i in self.testCases:
			self.message["change"] = False
			self.message["message"] = self.body*i
			j = 0
			while j<1000:
				self.message["time"] = time.time()
				rospy.loginfo(self.message)
				self.message_publisher.publish(json.dumps(self.message))
				j+=1
			self.message["change"] = True
			self.message_publisher.publish(json.dumps(self.message))
			time.sleep(1)
		self.message = {}
		while not rospy.is_shutdown():
			print("QUIT")
			rospy.loginfo(self.message)
			self.message_publisher.publish(json.dumps(self.message))
		

if __name__ == "__main__":
	publ = LatencyPub(1, 'PubNode', 'messageTopic', list(range(51, 99952, 500)))
	try:
		publ.messagePublisher()
	except rospy.ROSInterruptException:
		pass	



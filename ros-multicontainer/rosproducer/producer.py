#!/usr/bin/env python
import rospy
from std_msgs.msg import String
message_publisher = rospy.Publisher("test", String, queue_size=10)
rospy.init_node("pub", anonymous = True)
print("Sending now")
for i in range(100):
    message_publisher.publish(str(i))

print("Sending over")
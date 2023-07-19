#!/usr/bin/env python
import rospy
from std_msgs.msg import String
def callback(msg):
    print(msg)

print("receiving")
rospy.init_node("sub", anonymous = True)
rospy.Subscriber("test", String, callback)
rospy.spin()
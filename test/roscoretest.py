from Roscore import Roscore
import time
import rospy
if __name__ == "__main__":
	
	r = Roscore()
	
	try:
		print(rospy.get_master().getPid())
	except:
		r.run()
	print(rospy.get_master().getPid())
	
	
	

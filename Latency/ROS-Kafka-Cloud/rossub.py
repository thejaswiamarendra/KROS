import rospy
from std_msgs.msg import String
import time
import sys
import json
from statistics import mean
import pandas as pd

class LatencySub():
        def __init__(self, nodeName, topic):
                rospy.init_node(nodeName, anonymous = False)
                self.topic = topic
                self.valueList = []
                self.averages = {}
                self.prev = None
        def messageSubscriber(self):

                rospy.Subscriber(self.topic, String, self.callback)
                rospy.spin()
        def callback(self, data):
                if(len(json.loads(data.data))==0):
                        print(self.averages)
                        df = pd.DataFrame(columns = ["bytes", "time"])
                        df["bytes"] = self.averages.keys()
                        df["time"] = self.averages.values()
                        df.to_csv("rosvalues.csv")
                        sys.exit()
                else:
                        if(len(self.valueList)==0):
                                print(self.averages)

                        if(json.loads(data.data)["change"]):
                                self.averages[sys.getsizeof(json.loads(data.data)["message"])] = mean(self.valueList)
                                self.valueList = []
                        else:
                                self.valueList.append((time.time() - float(json.loads(data.data)["time"]))*1000)
                        #print((time.time() - float(json.loads(data.data)["time"]))*1000)



if __name__ == '__main__':
        sub = LatencySub('messageSubNode', 'messageTopic')
        try:
                sub.messageSubscriber()
        except rospy.ROSInterruptException:

                pass

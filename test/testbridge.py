from KROSbridge import KROSbridge
import json
from turtlesim.msg import Pose

def Convert(tup, di):
    for a, b in tup:
        di.setdefault(a, []).append(b)
    return di
    
def callback(msg):
    #print(str(msg).split('\n'))
    msg = str(msg).split('\n')
    lot = [(x.split(': ')[0], x.split(': ')[1]) for x in msg]
    msg = Convert(lot, dict())
    return msg
if __name__ == "__main__":
	cloud_conf = {
	    'bootstrap.servers': 'pkc-41p56.asia-south1.gcp.confluent.cloud:9092',
	    'sasl.username': 'J2LXJ2BZT5ZA63EE',
	    'sasl.password': 'qrnWDawOdsWxJ/l2i/LOX6yFsX+RFS9Hs2EYQHi/p0wcTVfxlX2FxlmLPBKdABTX',
	    'security.protocol': 'SASL_SSL',
	    'sasl.mechanism': 'PLAIN',
	    'value.serializer':lambda x: json.dumps(callback(x)).encode('utf-8')
		}
	b = KROSbridge('topic0', cloud_conf = cloud_conf, local_topic = '/turtle1/pose', datatype = Pose)
	b.run()

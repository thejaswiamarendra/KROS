from KROSsubscriber import KROSsubscriber

def f1(x):
	print("ROS - ", x)
	
def f2(x):
	print("Kafka - ", x.value)
if __name__ == "__main__":
	cloud_conf = {
            'bootstrap.servers': 'pkc-41p56.asia-south1.gcp.confluent.cloud:9092',
            'sasl.username': 'J2LXJ2BZT5ZA63EE',
            'sasl.password': 'qrnWDawOdsWxJ/l2i/LOX6yFsX+RFS9Hs2EYQHi/p0wcTVfxlX2FxlmLPBKdABTX',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'value.deserializer':lambda x: x.decode('utf-8')
        }
        
	ksub = KROSsubscriber(cloud_conf = cloud_conf, topic = "topic0", local_callback = None, cloud_callback = f2)
	
	ksub.subscribe()

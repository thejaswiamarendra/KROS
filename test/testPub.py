from KROSpublisher import KROSpublisher
import json
if __name__ == "__main__":
	cloud_conf = {
	    'bootstrap.servers': 'pkc-41p56.asia-south1.gcp.confluent.cloud:9092',
	    'sasl.username': 'J2LXJ2BZT5ZA63EE',
	    'sasl.password': 'qrnWDawOdsWxJ/l2i/LOX6yFsX+RFS9Hs2EYQHi/p0wcTVfxlX2FxlmLPBKdABTX',
	    'security.protocol': 'SASL_SSL',
	    'sasl.mechanism': 'PLAIN',
	    'value.serializer':lambda x: json.dumps(x).encode('utf-8')
		}
	
	kpub = KROSpublisher(cloud_conf = cloud_conf, topic = "topic0")
	
	print(kpub.get_topic())
	print(kpub.get_cloud_conf())
	
	
	for i in range(1000):
		kpub.publish("hello")

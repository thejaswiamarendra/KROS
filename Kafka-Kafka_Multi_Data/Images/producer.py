# import cv2
# from kafka import KafkaProducer
# producer=KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(0,10,1))
# image = cv2.imread("50kb.jpg")
# ret, buffer = cv2.imencode('.jpg', image)
# for i in range(300):
#         producer.send("ImgTopic",buffer.tobytes())



#producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10, 1))


import cv2
from kafka import KafkaProducer
import time
import json

   
class send_data:
        def __init__(self):
                self.producer = KafkaProducer(bootstrap_servers=conf['bootstrap.servers'],
                                 sasl_plain_username=conf['sasl.username'],
                                 sasl_plain_password=conf['sasl.password'],
                                 security_protocol=conf['security.protocol'],
                                 sasl_mechanism=conf['sasl.mechanism'],
                                 value_serializer=conf['value.serializer'])
                                       
        def send(self):
                image = cv2.imread("500kb.jpg")
                ret, buffer = cv2.imencode('.jpg', image)

                for i in range(1000):
                        if i!=999:
                                print(i)
                                timestamp = time.time()  # Get the current timestamp
                                message_value = buffer.tobytes()  # Image data as the message value
                                message = str(timestamp).encode() + b":" + message_value  # Combine timestamp and image data
                                self.producer.send("ImgTopic", message)
                        else:
                                # dummy_msg = 'End'
                                # #dummy_bytes = b'\x00\x01\x02\x03\x04\x05'
                                # self.producer.send("ImgTopic",dummy_msg.encode('ASCII'))
                                # timestamp = time.time()  # Get the current timestamp
                                # message_value = b'End'  # Image data as the message value
                                # message = str(timestamp).encode() + b":" + message_value  # Combine timestamp and image data
                                # self.producer.send("ImgTopic", message)
                                print('done')
    
if __name__ == "__main__":
        conf = {
        'bootstrap.servers': 'pkc-41p56.asia-south1.gcp.confluent.cloud:9092',
        'sasl.username': 'J2LXJ2BZT5ZA63EE',
        'sasl.password': 'qrnWDawOdsWxJ/l2i/LOX6yFsX+RFS9Hs2EYQHi/p0wcTVfxlX2FxlmLPBKdABTX',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'value.serializer':lambda x:x
                }
        obj=send_data()
        obj.send()

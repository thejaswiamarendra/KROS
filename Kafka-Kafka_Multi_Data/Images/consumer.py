# from PIL import Image
# from io import BytesIO
# from kafka import KafkaConsumer
# consumer = KafkaConsumer("ImgTopic",bootstrap_servers=['localhost:9092'],
#                         api_version=(0,10,1))

# for message in consumer:
#     stream = BytesIO(message.value)
#     image = Image.open(stream).convert("RGBA")
#     print(image.size)
#     stream.close()
#     image.show()

from PIL import Image
from io import BytesIO
from kafka import KafkaConsumer
import time
import json

#consumer = KafkaConsumer("ImgTopic", bootstrap_servers=['localhost:9092'], api_version=(0, 10, 1))

class recv_data:
        def __init__(self):
                self.consumer = KafkaConsumer("ImgTopic",
                                 bootstrap_servers=conf['bootstrap.servers'],
                                 sasl_plain_username=conf['sasl.username'],
                                 sasl_plain_password=conf['sasl.password'],
                                 security_protocol=conf['security.protocol'],
                                 sasl_mechanism=conf['sasl.mechanism'],
                                 value_deserializer=conf['value.deserializer'])
                self.values=[]
                self.avgs={}
                                       
        def recv(self):
                for message in self.consumer:
                        # Split the message value into timestamp and image data
                        timestamp, image_data = message.value.split(b":", 1)
                        
                        # Convert timestamp from bytes to integer
                        timestamp = float(timestamp.decode())
                        
                        # Create a BytesIO stream from the image data
                        stream = BytesIO(image_data)
                        
                        # Open the image from the stream and convert to RGBA
                        image = Image.open(stream).convert("RGBA")
                        
                        #Latency
                        #print((time.time()-timestamp)*1000)
                        self.values.append((time.time()-timestamp)*1000)
                        #image.show()
                        # Process the image or perform any desired operations
                        # For example, you can save the image with the timestamp as the filename:
                        #image.save(f"{timestamp}.png")
                        
                        # Close the stream
                        stream.close()
                        avg_latency=sum(self.values)/len(self.values)
                        print("Average Latency =",avg_latency)

                
    
if __name__ == "__main__":
        conf = {
            'bootstrap.servers': 'pkc-41p56.asia-south1.gcp.confluent.cloud:9092',
            'sasl.username': 'J2LXJ2BZT5ZA63EE',
            'sasl.password': 'qrnWDawOdsWxJ/l2i/LOX6yFsX+RFS9Hs2EYQHi/p0wcTVfxlX2FxlmLPBKdABTX',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'value.deserializer':lambda x: x
                }
        obj=recv_data()
        obj.recv()

from kafka import KafkaProducer, KafkaConsumer
import json
import time
def serializer(message):
    return json.dumps(message).encode("utf-8")

producer = KafkaProducer(bootstrap_servers = ["localhost:9092"], api_version=(0,11,5), value_serializer=serializer)
time.sleep(3)
# consumer = KafkaConsumer("test1", bootstrap_servers = "localhost:9092")
# while not producer.bootstrap_connected():

#     producer = KafkaProducer(bootstrap_servers = "localhost:9092", api_version=(0,11,5), value_serializer=serializer)
#     print(producer.bootstrap_connected())
#     print(consumer.bootstrap_connected())


print(producer.bootstrap_connected())
f = 0
t = 0
while True:
    x = producer.send("test1", "hello")
    if(x.succeeded()):
        t+=1
    else:
        f+=1
    print("Sent - ", t, " Failed - ", f, end = '\r')

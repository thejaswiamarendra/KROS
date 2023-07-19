from kafka import KafkaConsumer
from kafka import KafkaAdminClient

admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092')
consumer = KafkaConsumer("test1", bootstrap_servers = "localhost:9092", auto_offset_reset = "latest")

print(consumer.bootstrap_connected())
metadata = admin_client.list_consumer_groups()
print(metadata)
count = 0
for msg in consumer:
    #print(admin_client.list_consumer_groups())
    count+=1
    print("Number of messages received : ", count, end = '\r')
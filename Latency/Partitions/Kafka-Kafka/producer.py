from confluent_kafka import Producer

# Define the Kafka producer configuration
producer_config = {
    'bootstrap.servers': 'localhost"9092'
}

# Create the Kafka producer
producer = Producer(producer_config)

# Define the topic to produce messages to
topic = 'testpId'

# Produce messages to the topic
for i in range(10):
    value = "Message {}".format(i+1)
    # Produce the message to the topic
    producer.produce(topic=topic, value=value.encode('utf-8'))

    # Flush the producer to ensure the message is sent
    producer.flush()

    print("Produced message: {}".format(value))

# Close the producer to release resources
producer.close()


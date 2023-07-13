from confluent_kafka import Consumer, KafkaException, TopicPartition

# Define the Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost"9092',
    'group.id': 'test_partition_ID',
    'auto.offset.reset': 'earliest'
}

# Create the Kafka consumer
consumer = Consumer(consumer_config)

# Subscribe to the topic(s)
consumer.subscribe(['testpId'])

# Consume messages from the topic
try:
    while True:
        message = consumer.poll(1.0)  # Poll for new messages for 1 second

        if message is None:
            continue

        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                # Reached the end of the partition
                print("Reached end of partition: %s - %s" % (message.topic(), message.partition()))
                continue
            else:
                # Handle the error
                raise KafkaException(message.error())

        # Process the message
        print("Received message: {}".format(message.value().decode('utf-8')))

        # Retrieve the partition ID of the message
        topic_partition = TopicPartition(message.topic(), message.partition())
        consumer.assign([topic_partition])
        committed_offsets = consumer.committed(topic_partition)
        partition_id = committed_offsets.partition
        print("Partition ID: {}".format(partition_id))

except KeyboardInterrupt:
    # User interrupted the program
    pass

finally:
    # Close the consumer to release resources
    consumer.close()


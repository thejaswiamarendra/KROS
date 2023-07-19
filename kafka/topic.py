from kafka.admin import KafkaAdminClient, NewTopic


admin_client = KafkaAdminClient(
    bootstrap_servers="127.0.0.1:9092"
)

topic_list = []
topic_list.append(NewTopic(name="testtopic", num_partitions=1, replication_factor=1))
x = admin_client.create_topics(new_topics=topic_list, validate_only=False)
print(x)
print(admin_client.list_consumer_groups())
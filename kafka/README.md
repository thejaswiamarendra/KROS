# Kafka Broker Image

## How to run
1. Change into kafkamaster directory

2. Build the docker image
```
docker build -t kafkamaster .
```

3. Run the docker image
```
docker run -p 9092:9092 -p 2181:2181 -it --name kafkacont kafkamaster
```
4. Once Kafka is up and running, run kafkaconsumer/consumer.py and kafkaproducer/producer.py in seperate terminals. You should see the messages working
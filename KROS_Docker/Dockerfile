FROM ubuntu:16.04

# Set non-interactive mode during installation
ENV DEBIAN_FRONTEND noninteractive

# Update package list and install necessary dependencies
RUN apt-get update && apt-get install -y \
    curl \
    gnupg

# Install Java JDK
RUN apt-get install -y openjdk-8-jdk

RUN apt install net-tools

# Set up the ROS repository
RUN echo "deb http://packages.ros.org/ros/ubuntu xenial main" > /etc/apt/sources.list.d/ros-latest.list \
    && curl -s https://raw.githubusercontent.com/ros/rosdistro/master/ros.asc | apt-key add -

# Install ROS Kinetic
RUN apt-get update && apt-get install -y \
    ros-kinetic-desktop-full

# Set up ROS environment variables
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV ROS_DISTRO kinetic
ENV ROS_MASTER_URI http://localhost:11311
ENV ROS_HOSTNAME localhost

# Set up Kafka environment variables
ENV KAFKA_VERSION=3.5.0
ENV KAFKA_SCALA_VERSION=2.13

# Download and extract Kafka
RUN apt-get install -y wget \
    && wget https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}.tgz \
    && tar -xzf kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /opt \
    && rm kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}.tgz \
    && mv /opt/kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka

# Add Kafka binaries to the PATH
ENV PATH=${PATH}:/opt/kafka/bin

# Expose Kafka and ROS ports
EXPOSE 9092
EXPOSE 11311
EXPOSE 2181

COPY KROSbridge.py /home/KROS/KROSbridge.py
COPY KROSpublisher.py /home/KROS/KROSpublisher.py
COPY KROSsubscriber.py /home/KROS/KROSsubscriber.py
COPY Roscore.py /home/KROS/Roscore.py

COPY roskafka.sh /home/roskafka.sh

WORKDIR /home

RUN chmod +x roskafka.sh

CMD ["./roskafka.sh"]
# Start roscore, ZooKeeper, and Kafka
# CMD /bin/bash -c "source /opt/ros/kinetic/setup.bash && roscore" & /opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties & /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties

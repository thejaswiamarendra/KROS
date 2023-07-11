#!/bin/bash

source /opt/ros/kinetic/setup.bash && roscore &
/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties &
/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
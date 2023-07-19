#!/bin/bash

# Run the consumer.py script
source /rosws/devel/setup.bash && rosrun rospkg consumer.py &


# Wait for 10 seconds
sleep 10

# Run the producer.py script
source /rosws/devel/setup.bash && rosrun rospkg producer.py &

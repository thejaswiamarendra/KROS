FROM ubuntu:16.04
RUN rm /bin/sh && ln -s /bin/bash /bin/sh
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y \
    curl \
    gnupg

RUN apt install net-tools

# Set up the ROS repository
RUN echo "deb http://packages.ros.org/ros/ubuntu xenial main" > /etc/apt/sources.list.d/ros-latest.list \
    && curl -s https://raw.githubusercontent.com/ros/rosdistro/master/ros.asc | apt-key add -

# Install ROS Kinetic
RUN apt-get update && apt-get install -y \ros-kinetic-desktop-full

#dos2unix is used to convert files in windows to linux format. I was getting an error called "'python/r' module not found". This fixed the error
RUN apt install dos2unix

ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV ROS_DISTRO kinetic
ENV ROS_MASTER_URI http://localhost:11311
ENV ROS_HOSTNAME localhost

EXPOSE 11311


RUN source /opt/ros/kinetic/setup.bash


RUN echo "source /rosws/devel/setup.bash" >> ~/.bashrc

# Making directories for catkin workspace
RUN mkdir rosws && cd rosws && mkdir src

# Creating catkin workspace
RUN source /opt/ros/kinetic/setup.bash && catkin_make -C rosws
RUN source /rosws/devel/setup.bash
# Creating catkin package
RUN cd rosws && cd src && catkin_create_pkg rospkg std_msgs rospy roscpp
RUN source /opt/ros/kinetic/setup.bash && catkin_make -C rosws
COPY consumer.py rosws/src/rospkg/src/consumer.py
# Convert the producer.py file
RUN dos2unix rosws/src/rospkg/src/consumer.py

# CMD source /rosws/devel/setup.bash && rosrun rospkg consumer.py
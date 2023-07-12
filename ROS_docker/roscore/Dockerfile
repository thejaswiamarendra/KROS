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

ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV ROS_DISTRO kinetic
# ENV ROS_MASTER_URI http://localhost:11311
# ENV ROS_HOSTNAME localhost

EXPOSE 11311

# CMD source ./opt/ros/kinetic/setup.bash && roscore

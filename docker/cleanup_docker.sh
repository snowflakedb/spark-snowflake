#!/bin/bash

# Kill all running containers
docker container kill $(docker ps -a -q)

# Remove all containers
docker rm $(docker ps -a -q)

# Remove image
docker rmi spark-base:2.4.5

# Show status
docker ps -a

docker image ls -a

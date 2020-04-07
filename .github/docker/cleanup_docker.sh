#!/bin/bash

# Kill all running containers
docker container kill $(docker ps -q)

# Remove all containers
docker rm $(docker ps -a -q)

# Remove image
docker rmi $DOCKER_IMAGE_TAG

# Show status
docker ps -a

docker image ls -a

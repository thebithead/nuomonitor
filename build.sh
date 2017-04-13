#!/bin/bash

TAG=support.bo2.nuodb.com:5000/nuomonitor:latest

docker rmi ${TAG}
docker build --tag ${TAG} .

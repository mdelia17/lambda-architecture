#!/bin/bash

docker exec -it hive-server apt-get update -y
docker exec -it hive-server apt-get install -y python3
docker exec -it namenode apt-get update -y
docker exec -it namenode apt-get install -y python3
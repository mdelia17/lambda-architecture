#!/bin/bash

docker exec -it hive-server rm -r /hive
docker cp ./hive-job hive-server:hive
# docker exec -it hive-server chmod 777 /hive-job/*

docker exec -it hive-server apt-get update -y
docker exec -it hive-server apt-get install -y python3
#!/bin/bash

docker exec -it hive-server rm -r /hive
docker cp ./hive hive-server:hive
docker exec -it namenode rm -r mapreduce
docker cp ./mapreduce namenode:mapreduce
docker exec -it spark-master rm -r /streaming
docker exec -it spark-master rm -r /batch
docker cp ./streaming spark-master:/streaming
docker cp ./batch spark-master:/batch
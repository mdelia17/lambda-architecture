@echo off

docker exec -it namenode rm -r mapreduce
docker cp ./mapreduce namenode:mapreduce
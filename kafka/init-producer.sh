#!/bin/bash

docker exec -it kafka-connect-prod rm -r /tmp/kafka-connect-prod/network-data
docker exec -it kafka-connect-prod mkdir -p /tmp/kafka-connect-prod/network-data

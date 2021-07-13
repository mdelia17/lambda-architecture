#!/bin/bash

docker exec -it connect rm -r /tmp/kafka-connect/examples
docker exec -it connect mkdir -p /tmp/kafka-connect/examples

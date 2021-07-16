#!/bin/bash

docker exec -it namenode /bin/bash hdfs dfs -chmod 777 /
echo 'starting consumer'
curl -i -X PUT -H  "Content-Type:application/json" http://localhost:9067/connectors/kafka-connect-cons-00/config -d @kafka-connect-cons-config.json

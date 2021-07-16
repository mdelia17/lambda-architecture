@ECHO OFF

docker exec -it namenode /bin/bash hdfs dfs -chmod 777 /
powershell -Command "Invoke-RestMethod -Method Put -Uri http://localhost:9067/connectors/kafka-connect-cons-00/config -ContentType "application/json" -Body $(get-content kafka-connect-cons-config.json -raw)"
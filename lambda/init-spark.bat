@ECHO OFF

docker exec -it spark-master rm -r /streaming
docker exec -it spark-master rm -r /batch
docker cp ./streaming spark-master:/streaming
docker cp ./batch spark-master:/batch
docker cp ./spark-cassandra-connector-assembly-3.0.1-5-g8bab1061.jar spark-master:/spark/jars/spark-cassandra-connector-assembly-3.0.1-5-g8bab1061.jar
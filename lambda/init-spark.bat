@ECHO OFF

docker exec -it spark-master rm -r /streaming
docker exec -it spark-master rm -r /batch
docker cp ./streaming spark-master:/streaming
docker cp ./batch spark-master:/batch

@echo off

docker exec -it hive-server rm -r /hive
docker cp ./hive hive-server:hive
@REM docker exec -it hive-server chmod 777 /hive/*

docker exec -it hive-server apt-get update -y
docker exec -it hive-server apt-get install -y python3
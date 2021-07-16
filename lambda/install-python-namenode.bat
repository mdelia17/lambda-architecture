@ECHO OFF

docker exec -it namenode apt-get update -y
docker exec -it namenode apt-get install -y python3
@ECHO OFF
docker run -it --rm --network net-cassandra bitnami/cassandra:3.11.10 cqlsh --username cassandra --password cassandra cas-node1
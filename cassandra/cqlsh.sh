#!/bin/bash

docker run -it --rm --network net-cassandra bitnami/cassandra:3.11.10 cqlsh --username cassandra --password cassandra cassandra-1

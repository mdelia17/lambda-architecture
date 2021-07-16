#!/bin/bash

usage()
{
	echo 'Usage: start-streaming.sh <streaming job name>'
	exit 1
}

if [ $# -eq 0 ]; then
	usage
fi

echo 'executing streaming job '$1
docker exec -it spark-master rm -r /tmp/$1
docker exec -i -t -u root spark-master /bin/bash /spark/bin/spark-submit --master local[4] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 ./streaming/$1.py

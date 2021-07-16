#!/bin/bash

usage()
{
	echo 'Usage: start-batch.sh <batch job name>'
	exit 1
}

if [ $# -eq 0 ]; then
	usage
fi

echo 'executing batch job '$1
docker exec -it namenode /bin/bash hdfs dfs -rm -r /output
docker exec -i -t -u root spark-master /bin/bash /spark/bin/spark-submit --master local[4] /batch/$1.py --input_path hdfs://namenode:8020/topics/network-data/partition=0/* --output_path hdfs://namenode:8020/output
docker exec -it namenode /bin/bash hdfs dfs -cat /output/part-00000
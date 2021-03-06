#!/bin/bash

usage()
{
	echo 'Usage: start-job-mapreduce.sh <mapreduce job name>'
	exit 1
}

if [ $# -eq 0 ]; then
	usage
fi

echo executing mapreduce job $1
docker exec -it -u root namenode /bin/bash hdfs dfs -rm -r /output
docker exec -it -u root namenode /bin/bash /opt/hadoop-3.2.1/bin/hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -D mapred.reduce.tasks=1 -mapper mapreduce/mapper_$1.py -reducer mapreduce/reducer_$1.py -input /topics/network-data/partition=0/* -output /output

docker exec -it -u root namenode /bin/bash hdfs dfs -cat /output/*
docker exec -it -u root namenode /bin/bash hdfs dfs -rm -r /output
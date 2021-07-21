#!/bin/bash

usage()
{
	echo 'Usage: start-job-hive.sh <hive job name>'
	exit 1
}

if [ $# -eq 0 ]; then
	usage
fi

echo 'executing hive job '$1
docker exec -it -u root namenode /bin/bash hdfs dfs -rm -r /user/hive/warehouse/data
docker exec -it -u root namenode /bin/bash hdfs dfs -mkdir /user/hive/warehouse/data
docker exec -it -u root namenode /bin/bash hdfs dfs -cp /topics/network-data/partition=0/* /user/hive/warehouse/data
docker exec -i -t -u root hive-server /bin/bash /opt/hive/bin/hive --f /hive/$1.hql
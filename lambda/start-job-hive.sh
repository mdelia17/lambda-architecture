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
docker exec -i -t -u root hive-server /bin/bash /opt/hive/bin/hive --f /hive/$1.hql
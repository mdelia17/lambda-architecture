@echo off

echo executing mapreduce job %1
docker exec -it -u root namenode /bin/bash /opt/hadoop-3.2.1/bin/hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -D mapred.reduce.tasks=1 -mapper mapreduce/mapper_%1.py -reducer mapreduce/reducer%1.py -input /topics/network-data/partition=0/network-data+0+0000000000+0000000098.json -output /output

docker exec -it -u root namenode /bin/bash hdfs dfs -cat /output/*
docker exec -it -u root namenode /bin/bash hdfs dfs -rm -r /output
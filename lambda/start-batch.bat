@ECHO OFF
ECHO executing batch job %1
docker exec -it namenode /bin/bash hdfs dfs -rm -r /output
docker exec -i -t -u root spark-master /bin/bash /spark/bin/spark-submit --master local[4] /batch/%1.py --input_path hdfs://namenode:8020/topics/connect-file-pulse-quickstart-csv/partition=0/* --output_path hdfs://namenode:8020/output
docker exec -it namenode /bin/bash hdfs dfs -cat /output/part-00000
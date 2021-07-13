# Copiare tutti i file necessari dentro spark master e il namenode
docker cp ./streaming-job.py spark-master:/streaming-job.py

# copia del jar per il connettore di cassandra
docker cp ./spark-cassandra-connector-assembly-3.0.1-5-g8bab1061.jar spark-master:/spark/jars/spark-cassandra-connector-assembly-3.0.1-5-g8bab1061.jar

# esecuzione job streaming
docker exec -i -t -u root spark-master /bin/bash /spark/bin/spark-submit --master local[4] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 ./streaming/cname.py



# copia del job di test
docker cp ./spark-cassandra-connector-test.py spark-master:/spark-cassandra-connector-test.py

# esecuzione del job batch al nodo spark-master
docker exec -i -t -u root spark-master /bin/bash /spark/bin/spark-submit --master local ./spark-cassandra-connector-test.py
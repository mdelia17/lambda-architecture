# controllare che il contenitore kafka-connect scarica i 2 connettori (vedere i log)

# controllare lo stato dei connettori (ci devono stare HDFS e SPoolDir)
curl -s localhost:9067/connector-plugins|jq '.[].class'

# entrare dentro il namenode
docker exec -it namenode /bin/bash

# cambiare i permessi della cartella / altrimenti non si può scrivere
hdfs dfs -chmod 777 /

# uscire dal namenode (comando exit)

# verificare se i messaggi sono presenti nel canale
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic connect-file-pulse-quickstart-csv --from-beginning

# connettore per HDFS se si vuole usare con connettore 2 (VEDERE BENE CHE COSA FA tasks.max: https://docs.confluent.io/kafka-connect-hdfs/current/overview.html  https://docs.confluent.io/2.0.0/connect/userguide.html)
curl -i -X PUT -H  "Content-Type:application/json" http://localhost:9067/connectors/sink-hdfs-network-data-00/config -d '{"connector.class": "io.confluent.connect.hdfs3.Hdfs3SinkConnector","value.converter": "org.apache.kafka.connect.json.JsonConverter","tasks.max": "1","topics": "connect-file-pulse-quickstart-csv","hdfs.url": "hdfs://namenode:8020","flush.size": "99","confluent.topic.bootstrap.servers": "broker:29092","format.class": "io.confluent.connect.hdfs3.json.JsonFormat"}'

# controllare i file dentro HDFS, quindi entrare dentro il namenode (docker exec -it namenode /bin/bash):
# 1. prendere un file su HDFS, ad esempio:
hdfs dfs -get /topics/network_data/partition=0/network_data+0+0000000000+0000000098.avro

hdfs dfs -get /topics/network_data/partition=0/network_data+0+0000000099+0000000197.avro
# 2. scaricare un jar di avro per leggere il file:
wget https://repo1.maven.org/maven2/org/apache/avro/avro-tools/1.8.2/avro-tools-1.8.2.jar

curl https://repo1.maven.org/maven2/org/apache/avro/avro-tools/1.8.2/avro-tools-1.8.2.jar -o ./avro-tools-1.8.2.jar

# 3. leggere il file:
java -jar avro-tools-1.8.2.jar tojson network_data+0+0000000000+0000000098.avro
java -jar avro-tools-1.8.2.jar tojson network_data+0+0000000099+0000000197.avro
# uscire (comando exit) e distruggere i contenitori:
docker-compose down

# spostare il csv dalla cartella processed alla cartella unprocessed per la prossima esecuzione
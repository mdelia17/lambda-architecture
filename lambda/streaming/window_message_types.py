import sys
from pyspark.streaming import StreamingContext
from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from cassandra.cluster import Cluster
from pyspark.sql.types import *

def request(line):
    return line[1][2] == "DNS"
    
def filter_field(line):
        words = line[1][4].strip().split(" ")
        return ((line[0][0], line[0][1], words[0]+ " "+ words[1]), 1)

def foreach_batch_function(df, epoch_id):
    try:
        # df.show(df.count(), False)
        lines_stream = df.rdd.map(list) 
        # lines_stream.coalesce(1,True).saveAsTextFile("file:///Users/gianluca/Desktop/Big-Data/secondo-progetto/"+str(epoch_id)) 
        lines_stream = lines_stream.filter(request)
        lines_stream = lines_stream.map(filter_field)
        aggregate_stream = lines_stream.reduceByKey(lambda a, b: a+b) 
        aggregate_stream = aggregate_stream.map(lambda line: (line[0][0], line[0][1], line[0][2], line[1]))
        
        for elem in aggregate_stream.collect():
            print(elem)
            # per ogni elem viene fatta la query e si ottengono tutte le righe che soddisfano la clausola where (è al massimo una perché la query è fatta sulla chiave)
            address_lookup_stmt = session.prepare("SELECT start, end, type, count  FROM dns_streaming.window_message_types WHERE start=? AND end=? AND type=?")
            rows = session.execute(address_lookup_stmt, [elem[0], elem[1], elem[2]])
            # fa la insert dell'elem corrente, se già esiste nel db viene sovrascritto
            session.execute("INSERT INTO dns_streaming.window_message_types (start, end, type, count) VALUES (%s, %s, %s, %s)", (elem[0], elem[1], elem[2], int(elem[3])))
            # se l'elem stava nel db viene fatto un inserimento con i campi aggiornati
            for row in rows: 
                # print(row)
                new_count = row.count + int(elem[3])
                # print(new_count)
                session.execute("INSERT INTO dns_streaming.window_message_types (start, end, type, count) VALUES (%s, %s, %s, %s)", (elem[0], elem[1], elem[2], new_count))
    except: 
        pass

# initialize the SparkSession
spark = SparkSession \
    .builder \
    .appName("Structured Streaming application with Kafka") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

lines_DF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "network-data") \
    .option("startingOffsets","latest")\
    .load()

hosts = ['cassandra-1']
port = 9042

# this object models a Cassandra cluster
cluster = Cluster(contact_points=hosts, port=port)

# initialize a session to interact with the cluster:
session = cluster.connect(keyspace="dns_streaming",
                        wait_for_all_pools=True)

schema = StructType() \
        .add("schema", StringType()) \
        .add("payload", StructType().add("message", StringType())) \

lines_DF = lines_DF\
    .selectExpr("cast(value as string)", "timestamp")\

#lines_DF.printSchema()

lines_DF = lines_DF\
    .select(from_json(lines_DF.value, schema), "timestamp")\
    .select("from_json(value).payload.message", "timestamp")\

#lines_DF.printSchema()
lines_DF = lines_DF.select(split(lines_DF.message, ',').alias('fields'), lines_DF.timestamp)
#lines_DF.printSchema()

# Group the data by window and word and compute the count of each group
windowedCounts = lines_DF\
    .withWatermark("timestamp", "0 minutes") \
    .groupBy(
        window(lines_DF.timestamp, "20 seconds", "20 seconds"),
        lines_DF.fields)\
    .count()

#windowedCounts.printSchema()

windowedCounts = windowedCounts\
    .writeStream\
    .outputMode('update')\
    .option("checkpointLocation", "file:///tmp/window_message_types")\
    .foreachBatch(foreach_batch_function)\
    .start()  \
    .awaitTermination()
import sys
from pyspark.streaming import StreamingContext
from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from cassandra.cluster import Cluster
from pyspark.sql.types import *


def filter_std_query(line):
    info = line[1][4].strip().split(" ")
    return info[0] == "Standard"

def filter_field(line):
    info = line[1][4].strip().split(" ")
    if info[2] != "response":
        url = info[-1]
        domain = url.strip().split(".")
        if len(domain) > 1:
            string = domain[-2] + "." + domain[-1]
        else:
            string = domain[-1]
        return [[(line[0][0], line[0][1], "A", string), 1]]
    else:
        l = []
        for i in range(len(info)-1):
            if info[i] == "CNAME":
                l.append([(line[0][0], line[0][1], "CNAME", info[i+1]), 1])
            if info[i] == "NS":
                l.append([(line[0][0], line[0][1], "NS", info[i+1]), 1])
    return l

def foreach_batch_function(df, epoch_id):
    try:
        # df.show(df.count(), False)
        lines_stream = df.rdd.map(list) 
        # print(lines_stream.collect()) 
        lines_stream = lines_stream.filter(filter_std_query).map(filter_field)

        flatten_rdd = lines_stream.flatMap(lambda l: l)
        # print(flatten_rdd.collect())

        reduced_rdd = flatten_rdd.reduceByKey(lambda a,b: a + b).map(lambda l: [l[0][0], l[0][1], l[0][2], l[0][3], l[1]])
        # print(reduced_rdd.collect())
        
        for elem in reduced_rdd.collect():
            print(elem)
            # per ogni elem viene fatta la query e si ottengono tutte le righe che soddisfano la clausola where (è al massimo una perché la query è fatta sulla chiave)
            address_lookup_stmt = session.prepare("SELECT requests FROM dns_streaming.window_domain_requests_and_responses WHERE start=? AND end=? AND type=? AND request_response=?")
            rows = session.execute(address_lookup_stmt, [elem[0], elem[1], elem[2], elem[3]])
            # fa la insert dell'elem corrente, se già esiste nel db viene sovrascritto
            session.execute("INSERT INTO dns_streaming.window_domain_requests_and_responses (start, end, type, request_response, requests) VALUES (%s, %s, %s, %s, %s)", (elem[0], elem[1], elem[2], elem[3], int(elem[4])))
            # se l'elem stava nel db viene fatto un inserimento con i campi aggiornati
            for row in rows: 
                # print(row)
                new_requests = row.requests + int(elem[4])
                # print(new_count)
                session.execute("INSERT INTO dns_streaming.window_domain_requests_and_responses (start, end, type, request_response, requests) VALUES (%s, %s, %s, %s, %s)", (elem[0], elem[1], elem[2], elem[3], new_requests))
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
# lines_DF.printSchema()

lines_DF = lines_DF\
    .select(from_json(lines_DF.value, schema), "timestamp")\
    .select("from_json(value).payload.message", "timestamp")\
# lines_DF.printSchema()

lines_DF = lines_DF.select(split(lines_DF.message, ',').alias('fields'), lines_DF.timestamp)
# lines_DF.printSchema()

# Group the data by window and word and compute the count of each group
windowedCounts = lines_DF\
    .groupBy(window(lines_DF.timestamp, "20 seconds", "20 seconds"),lines_DF.fields)\
    .count()
# windowedCounts.printSchema()

windowedCounts = windowedCounts\
    .writeStream\
    .outputMode('update')\
    .option("checkpointLocation", "file:///tmp/window_requests_and_responses")\
    .foreachBatch(foreach_batch_function)\
    .start()  \
    .awaitTermination()
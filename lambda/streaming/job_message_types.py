import sys
from pyspark.streaming import StreamingContext
from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from cassandra.cluster import Cluster
from pyspark.sql.types import *

# initialize the SparkSession
spark = SparkSession \
    .builder \
    .appName("Structured Streaming application with Kafka") \
    .getOrCreate()
    # .config("spark.eventLog.enabled", "true")\
    # .config("spark.eventLog.dir", "file:///tmp/spark-events")\

spark.sparkContext.setLogLevel("ERROR")

lines_DF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "network-data") \
    .option("startingOffsets","latest")\
    .load()

def getSparkSessionInstance():
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .appName("SQL Example").master("local[*]")\
            .config("spark.sql.catalog.mycatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")\
            .config("spark.cassandra.connection.host", "cassandra-1")\
            .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")\
            .config("spark.cassandra.auth.username", "cassandra")\
            .config("spark.cassandra.auth.password", "cassandra")\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def filter_line(line): 
    fields = line[0].strip().split(",")
    # "IP" not in line[2] perché potrebbero esserci anche pacchetti IPv6
    return "TCP" not in fields[2] and "IP" not in fields[2]

def filter_errors(line):
    fields = line[0].strip().split(",")
    info = fields[4].strip().split(" ")
    # "IP" not in line[2] perché potrebbero esserci anche pacchetti IPv6
    types = ["Standard", "Dynamic", "Zone", "Server", "Inverse", "DNS"]
    return info[0] not in types

def filter_field(line):
    fields = line[0].strip().split(",")
    words = fields[4].split(" ")
    types = ["Standard", "Dynamic", "Zone", "Server", "Inverse", "DNS"]
    if words[0] not in types:
        return ("Error/Unknown operation", 1)
    return (words[0] + " " + words[1], 1)

def filter_errors_field(line):
    fields = line[0].strip().split(",")
    # si levano le virgolette ""
    # return (words[2][1:-1], words[3][1:-1], words[4][1:-1], [words[6][1:-1], 1])
    return (fields[0], 1)

def foreach_batch_function(df, epoch_id):
    try:
        lines_stream = df.rdd.map(list)
        filtered_stream = lines_stream.filter(filter_line)
        errors_stream = filtered_stream.filter(filter_errors)

        clean_stream = filtered_stream.map(filter_field)
        clean_errors_stream = errors_stream.map(filter_errors_field)

        agg_stream = clean_stream.reduceByKey(lambda a, b: a + b)
        agg_errors_stream = clean_errors_stream.reduceByKey(lambda a, b: a + b)
        
        for elem in agg_errors_stream.collect():
            print(elem)
            # per ogni elem viene fatta la query e si ottengono tutte le righe che soddisfano la clausola where (è al massimo una perché la query è fatta sulla chiave)
            address_lookup_stmt = session.prepare("SELECT errors  FROM dns_streaming.errors WHERE address=?")
            rows = session.execute(address_lookup_stmt, [elem[0]])
            # fa la insert dell'elem corrente, se già esiste nel db viene sovrascritto
            session.execute("INSERT INTO dns_streaming.errors (address, errors) VALUES (%s, %s)", (elem[0], int(elem[1])))
            # se l'elem stava nel db viene fatto un inserimento con i campi aggiornati
            for row in rows: 
                # print(row)
                new_errors = row.errors + int(elem[1])
                # print(new_count)
                session.execute("INSERT INTO dns_streaming.errors (address, errors) VALUES (%s, %s)", (elem[0], new_errors))
        
        # Get the singleton instance of SparkSession 
        spark = getSparkSessionInstance()
        # Convert to DataFrame
        columns = ["type", "packets"]
        df = agg_stream.toDF(columns)
        # df.printSchema()
        df.show(truncate=False)

        df.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(keyspace="dns_streaming", table="message_types")\
            .save()

    except:
        pass

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
    .selectExpr("cast(value as string)")\

lines_DF = lines_DF\
    .select(from_json(lines_DF.value, schema))\
    .select("from_json(value).payload.message")\
    .writeStream\
    .option("checkpointLocation", "file:///tmp/job_message_types")\
    .foreachBatch(foreach_batch_function)\
    .start()  \
    .awaitTermination() 

import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, from_json
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
    .option("subscribe", "connect-file-pulse-quickstart-csv") \
    .option("startingOffsets","latest")\
    .load()

def getSparkSessionInstance():
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .appName("SQL Example").master("local[*]")\
            .config("spark.sql.catalog.mycatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")\
            .config("spark.cassandra.connection.host", "cas-node1")\
            .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")\
            .config("spark.cassandra.auth.username", "cassandra")\
            .config("spark.cassandra.auth.password", "cassandra")\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def filter_field(line):
    words = line[0].strip().split(",")
    # si levano le virgolette ""
    # return (words[2][1:-1], words[3][1:-1], words[4][1:-1], [words[6][1:-1], 1])
    return (words[0], words[1], words[4])

def filter_line(line): 
    words = line[2].split(" ")
    # "IP" not in line[2] perché potrebbero esserci anche pacchetti IPv6
    # return len(words) > 2 and line[2] != "TCP" and "IP" not in line[2]
    return words[0] == 'Standard' or words[0] == 'DNS' or words[0] == 'Inverse'
    
def build_certain(line):
    words = line[2].split(" ")
    if ((words[0] == 'Standard' or words[0] == 'Inverse') and words[2] != "response") or (words[0] == 'DNS' and words[4] != "response"):
        return (line[1], [0, 1])
    else:
        return (line[0], [1, 0])

def build_uncertain(line):
    words = line[2].split(" ")
    if ((words[0] == 'Standard' or words[0] == 'Inverse') and words[2] != "response") or (words[0] == 'DNS' and words[4] != "response"):
        return (line[0], [1, 0])
    else:
        return (line[1], [0, 1])

def agg(line):
    if line[1][1] != None:
        return (line[0], line[1][0][0] + line[1][1][0], line[1][0][1] + line[1][1][1])
    else: 
        packets_sent, packets_received = line[1][0] 
        return (line[0], packets_sent, packets_received)

def foreach_batch_function(df, epoch_id):
    # df.show(2, False)
    lines_stream = df.rdd.map(list) 

    if not lines_stream.isEmpty():
        clean_stream = lines_stream.map(filter_field)
        filtered_stream = clean_stream.filter(filter_line)

        build_certain_stream = filtered_stream.map(build_certain)
        build_uncertain_stream = filtered_stream.map(build_uncertain)

        build_certain_agg_stream = build_certain_stream.reduceByKey(lambda a, b: [a[0] + b[0], a[1] + b[1]])
        build_uncertain_agg_stream =  build_uncertain_stream.reduceByKey(lambda a, b: [a[0] + b[0], a[1] + b[1]])

        join_stream = build_certain_agg_stream.leftOuterJoin(build_uncertain_agg_stream)

        final_stream = join_stream.map(agg)

        if not final_stream.isEmpty():
            # print(final_stream.collect())
            spark = getSparkSessionInstance()
            # Convert to DataFrame
            columns = ["address", "packets_sent", "packets_received"]
            df = final_stream.toDF(columns)
            # df.printSchema()
            df.show(truncate=False)

            df.write\
                .format("org.apache.spark.sql.cassandra")\
                .mode('append')\
                .options(keyspace="dns", table="nameserver")\
                .save()

schema = StructType() \
        .add("schema", StringType()) \
        .add("payload", StructType().add("message", StringType())) \
        

lines_DF = lines_DF\
    .selectExpr("cast(value as string)")\

lines_DF = lines_DF\
    .select(from_json(lines_DF.value, schema))\
    .select("from_json(value).payload.message")\
    .writeStream\
    .option("checkpointLocation", "file:///tmp/job_for_case_1_6_7")\
    .foreachBatch(foreach_batch_function)\
    .start()  \
    .awaitTermination() 

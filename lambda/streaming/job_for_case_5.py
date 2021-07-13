import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, from_json
from pyspark.sql.types import *

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


def filter_fields(line):
    words = line[0].strip().split(",")
    # si levano le virgolette ""
    return (words[0], words[1], words[4])

def server_status_filter(line):
    return line[2].startswith("Server status")

def server_status_mapper(line):
    if line[2].startswith("Server status request response"):
        return (line[0], [1,0])
    else:
        return (line[1], [0,1])

def foreach_batch_function(df, epoch_id):
    # df.show(2, False)
    lines_stream = df.rdd.map(list)
    if not lines_stream.isEmpty():
        fields_stream = lines_stream.map(filter_fields)
        # si considerano solo i messaggi di tipo "Server Status"
        server_status_stream = fields_stream.filter(server_status_filter)
        # server_status_stream.pprint()

        ip_stats_stream = server_status_stream.map(server_status_mapper)
        # ip_stats_stream.pprint()

        ip_stats_stream = ip_stats_stream.reduceByKey(lambda a,b: [a[0]+b[0], a[1]+b[1]])
        # ip_stats_stream.pprint()

        ip_stats_stream = ip_stats_stream.map(lambda a: (a[0], a[1][0], a[1][1]))

        if not ip_stats_stream.isEmpty():
            # Get the singleton instance of SparkSession 
            spark = getSparkSessionInstance()
            # Convert to DataFrame
            columns = ["address", "packets_sent", "packets_received"]
            df = ip_stats_stream.toDF(columns)
            df.printSchema()
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
    .option("checkpointLocation", "file:///tmp/job_for_case_5")\
    .foreachBatch(foreach_batch_function)\
    .start()  \
    .awaitTermination()
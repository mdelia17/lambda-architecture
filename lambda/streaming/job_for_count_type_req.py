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

def process(time, rdd):
    try:
        # Get the singleton instance of SparkSession 
        spark = getSparkSessionInstance()
        # Convert to DataFrame
        columns = ["type", "packets"]
        df = rdd.toDF(columns)
        df.printSchema()
        df.show(truncate=False)

        df.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(keyspace="dns", table="info")\
            .save()

        spark.sql("SELECT * FROM mycatalog.dns.info").show(truncate=False)
    except:
        pass

def filter_line(line): 
    fields = line[0].strip().split(",")
    # "IP" not in line[2] perché potrebbero esserci anche pacchetti IPv6
    return "TCP" not in fields[2] and "IP" not in fields[2]

def filter_field(line):
    fields = line.strip().split(",")
    words = fields[4].split(" ")
    # si levano le virgolette ""
    # return (words[2][1:-1], words[3][1:-1], words[4][1:-1], [words[6][1:-1], 1])
    return (words[0], 1)

def foreach_batch_function(df, epoch_id):
    # df.show(2, False)
    lines_stream = df.rdd.map(list)
    if not lines_stream.isEmpty():
        filtered_stream = lines_stream.filter(filter_line)
        clean_stream = filtered_stream.map(filter_field)
        agg_stream = clean_stream.reduceByKey(lambda a, b: a + b)
        if not agg_stream.isEmpty():
            # Get the singleton instance of SparkSession 
            spark = getSparkSessionInstance()
            # Convert to DataFrame
            columns = ["type", "packets"]
            df = agg_stream.toDF(columns)
            df.printSchema()
            df.show(truncate=False)

            df.write\
                .format("org.apache.spark.sql.cassandra")\
                .mode('append')\
                .options(keyspace="dns", table="info")\
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
    .option("checkpointLocation", "file:///tmp/job_for_count_type_req")\
    .foreachBatch(foreach_batch_function)\
    .start()  \
    .awaitTermination() 

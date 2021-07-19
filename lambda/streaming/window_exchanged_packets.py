from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, from_json, window
from pyspark.sql.types import *

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

def filter_dns(line):
    return line[1][2] == "DNS"  

def filter_field(line):
    return [(line[0][0], line[0][1], line[1][0], line[2], 0), (line[0][0], line[0][1], line[1][1], 0, line[2])]

def foreach_batch_function(df, epoch_id):
    try:
        # df.show(df.count(), False)
        lines_stream = df.rdd.map(list) 
        # print(lines_stream.collect()) 
        lines_stream = lines_stream.filter(filter_dns).map(filter_field)

        flat_rdd = lines_stream.flatMap(lambda l: l).map(lambda l: [(l[0], l[1], l[2]), [l[3], l[4]]])
        # print(flat_rdd.collect())

        reduced_rdd = flat_rdd.reduceByKey(lambda a,b: [a[0]+b[0], a[1]+b[1]]).map(lambda l: [l[0][0], l[0][1], l[0][2], l[1][0], l[1][1]])
        # print(reduced_rdd.collect())

        spark = getSparkSessionInstance()
        columns = ["start", "end", "address", "packets_sent", "packets_received"]
        df = reduced_rdd.toDF(columns)
        df.printSchema()
        df.show(df.count(), False)

        df.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(keyspace="dns_streaming", table="window_exchanged_packets")\
            .save()

        # spark.sql("SELECT * FROM mycatalog.dns.nameserver").show(truncate=False)
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
# lines_DF.printSchema()

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
    .option("checkpointLocation", "file:///tmp/window_exchanged_packets")\
    .foreachBatch(foreach_batch_function)\
    .start()  \
    .awaitTermination()
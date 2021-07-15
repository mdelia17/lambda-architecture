from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, from_json, window
from pyspark.sql.types import *

def getSparkSessionInstance():
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .appName("SQL Example").master("local[*]")\
            .config("spark.sql.catalog.mycatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")\
            .config("spark.cassandra.connection.host", "localhost")\
            .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def request(line):
    return line[1][2] == "DNS"
    
def filter_field(line):
        words = line[1][4].strip().split(" ")
        return ((line[0][0], line[0][1], words[0]+ " "+ words[1]), 1)

def foreach_batch_function(df, epoch_id):
    # df.show(df.count(), False)
    lines_stream = df.rdd.map(list) 
    # lines_stream.coalesce(1,True).saveAsTextFile("file:///Users/gianluca/Desktop/Big-Data/secondo-progetto/"+str(epoch_id)) 
    if not lines_stream.isEmpty():
        lines_stream = lines_stream.filter(request)
        lines_stream = lines_stream.map(filter_field)
        aggregate_stream = lines_stream.reduceByKey(lambda a, b: a+b) 
        aggregate_stream = aggregate_stream.map(lambda line: (line[0][0], line[0][1], line[0][2], line[1]))
        spark = getSparkSessionInstance()
        columns = ["start", "end", "type", "count"]
        df = aggregate_stream.toDF(columns)
        df.printSchema()
        df.show(df.count(), False)
        df.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(keyspace="dns", table="type_window")\
            .save()

        # spark.sql("SELECT * FROM mycatalog.dns.nameserver").show(truncate=False)

# initialize the SparkSession
spark = SparkSession \
    .builder \
    .appName("Structured Streaming application with Kafka") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

lines_DF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "connect-file-pulse-quickstart-csv") \
    .option("startingOffsets","earliest")\
    .load()

# spark.sql("SET -v").show(n=200, truncate=False)
lines_DF.printSchema()

schema = StructType() \
        .add("schema", StringType()) \
        .add("payload", StructType().add("message", StringType())) \

lines_DF = lines_DF\
    .selectExpr("cast(value as string)", "timestamp")\

lines_DF.printSchema()

lines_DF = lines_DF\
    .select(from_json(lines_DF.value, schema), "timestamp")\
    .select("from_json(value).payload.message", "timestamp")\

lines_DF.printSchema()
lines_DF = lines_DF.select(split(lines_DF.message, ',').alias('fields'), lines_DF.timestamp)
lines_DF.printSchema()

# Group the data by window and word and compute the count of each group
windowedCounts = lines_DF\
    .withWatermark("timestamp", "0 minutes") \
    .groupBy(
        window(lines_DF.timestamp, "20 seconds", "20 seconds"),
        lines_DF.fields)\
    .count()

windowedCounts.printSchema()

windowedCounts = windowedCounts\
    .writeStream\
    .outputMode('update')\
    .option("checkpointLocation", "file:///tmp/spark-events")\
    .foreachBatch(foreach_batch_function)\
    .start()  \
    .awaitTermination()
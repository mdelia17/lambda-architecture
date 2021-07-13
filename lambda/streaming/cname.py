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

def format_line(line):
    words = line[0].strip().split(",")
    # print(line)
    return [words[0], words[1], words[4]]

def request(line):
    words = line[2].split(" ")
    return "CNAME" in words

def cname_lookup(line):
    words = line[2].split(" ")
    cnames = ""
    for i in range(len(words)-1):
        if words[i] == "CNAME":
            cnames += " " +(words[i+1])
    return cnames

def foreach_batch_function(df, epoch_id):
    # df.show(2, False)
    lines_stream = df.rdd.map(list)
    if not lines_stream.isEmpty():
        # si tengono solo i campi necessari
        fields_stream = lines_stream.map(format_line)
        # fields_stream.pprint()

        # si prendono solo i pacchetti DNS contenenti i CNAME
        all_request_stream = fields_stream.filter(request)
        # all_request_stream.pprint()

        # cambio del contenuto del campo info nel record
        clear_request_stream = all_request_stream.map(cname_lookup)
        # clear_request_stream.pprint()

        cnames_stream = clear_request_stream.flatMap(lambda line: line.strip().split(" "))
        # cnames_stream.pprint()

        pairs_stream = cnames_stream.map(lambda cname: (cname,1))
        # pairs_stream.pprint()

        reduced_stream = pairs_stream.reduceByKey(lambda a, b: a + b)
        if not reduced_stream.isEmpty():
            # print(final_stream.collect())
            spark = getSparkSessionInstance()
            columns = ["cname", "count"]
            df = reduced_stream.toDF(columns)
            # df.printSchema()
            df.show(truncate=False)

            df.write\
                .format("org.apache.spark.sql.cassandra")\
                .mode('append')\
                .options(keyspace="dns", table="cname")\
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
    .option("checkpointLocation", "file:///tmp/cname")\
    .foreachBatch(foreach_batch_function)\
    .start()  \
    .awaitTermination() 

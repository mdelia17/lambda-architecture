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
    .option("subscribe", "network_data") \
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

def filter_field(line):
    words = line[0].strip().split(",")
    # si levano le virgolette ""
    return (words[0], words[1], words[4])

def request_response(line):
    words = line[2].split(" ")
    # forse qua era più giusto filtrare per "DNS"
    return words[0] == 'Standard' or words[0] == 'Unknown'

def set_key(line):
    source, destination, info = line
    infos = info.split(" ")
    if infos[0] == 'Standard':
        if infos[2] != 'response':
            id_query = infos[2]
            return ((id_query, source, destination), [info + " src: " + source + " dest: " + destination])
        else:
            # da notare che quando si ha una response gli indirizzi IP vengono invertiti, in modo da fare una reduceByKey dopo
            id_query = infos[3]
            return ((id_query, destination, source), [info + " src: " + source + " dest: " + destination])
    else:
        # qui si è nel caso "Unknown ..."
        id_query = infos[3][0:6]
        return ((id_query, source, destination), [info + " src: " + source + " dest: " + destination])

def foreach_batch_function(df, epoch_id):
    try:
        # df.show(2, False)
        lines_stream = df.rdd.map(list)
        fields_stream = lines_stream.map(filter_field)
        all_request_response_stream = fields_stream.filter(request_response)
        set_key_stream = all_request_response_stream.map(set_key)
        # qui viene fatta una concatenazione di liste
        aggregate_stream = set_key_stream.reduceByKey(lambda a, b: a + b)
        # penso che se la lista è solo 1, quindi si vede solo una richiesta o solo una risposta, significa che la richiesta o la risposta si trova in un altro file
        filter_stream = aggregate_stream.filter(lambda a: len(a[1]) > 1)
        # print(final_stream.collect())
        spark = getSparkSessionInstance()
        columns = ["conversation_id", "messages"]
        df = filter_stream.toDF(columns)
        # df.printSchema()
        df.show(truncate=False)

        df.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(keyspace="dns", table="conversations")\
            .save()
    except: 
        pass

schema = StructType() \
        .add("schema", StringType()) \
        .add("payload", StructType().add("message", StringType())) \
        

lines_DF = lines_DF\
    .selectExpr("cast(value as string)")\

lines_DF = lines_DF\
    .select(from_json(lines_DF.value, schema))\
    .select("from_json(value).payload.message")\
    .writeStream\
    .option("checkpointLocation", "file:///tmp/job_for_conversation")\
    .foreachBatch(foreach_batch_function)\
    .start()  \
    .awaitTermination() 
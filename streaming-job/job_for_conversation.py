import sys
import re

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: hdfs_wordcount.py <directory>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="PythonStreamingHDFSWordCount")
    ssc = StreamingContext(sc, 10)

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

    def process(time, rdd):
        try:
            # Get the singleton instance of SparkSession 
            spark = getSparkSessionInstance()
            # Convert to DataFrame
            columns = ["key", "messages"]
            df = rdd.toDF(columns)
            df.printSchema()
            df.show(truncate=False)

            df.write\
              .format("org.apache.spark.sql.cassandra")\
              .mode('append')\
              .options(keyspace="dns", table="conversation")\
              .save()

            spark.sql("SELECT * FROM mycatalog.dns.conversation").show(truncate=False)
        except:
            pass

    def filter_field(line):
        words = line.strip().split(",")
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

    lines_stream = ssc.textFileStream(sys.argv[1])
    print(sys.argv[1])

    # si vedono le conversazioni
    fields_stream = lines_stream.map(filter_field)
    all_request_response_stream = fields_stream.filter(request_response)
    set_key_stream = all_request_response_stream.map(set_key)
    # qui viene fatta una concatenazione di liste
    aggregate_stream = set_key_stream.reduceByKey(lambda a, b: a + b)
    # penso che se la lista è solo 1, quindi si vede solo una richiesta o solo una risposta, significa che la richiesta o la risposta si trova in un altro file
    filter_stream = aggregate_stream.filter(lambda a: len(a[1]) > 1)
 
    filter_stream.pprint()
    filter_stream.foreachRDD(process)
    
    ssc.start()
    ssc.awaitTermination()
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
            columns = ["address", "packets_sent", "packets_received"]
            df = rdd.toDF(columns)
            df.printSchema()
            df.show(truncate=False)

            df.write\
              .format("org.apache.spark.sql.cassandra")\
              .mode('append')\
              .options(keyspace="dns", table="nameserver")\
              .save()

            spark.sql("SELECT * FROM mycatalog.dns.nameserver").show(truncate=False)
        except:
            pass

    def filter_fields(line):
        words = line.strip().split(",")
        # si levano le virgolette ""
        return (words[0], words[1], words[4])

    def server_status_filter(line):
        return line[2].startswith("Server status")

    def server_status_mapper(line):
        if line[2].startswith("Server status request response"):
            return (line[0], [1,0])
        else:
            return (line[1], [0,1])
            
    lines_stream = ssc.textFileStream(sys.argv[1])

    fields_stream = lines_stream.map(filter_fields)
    # fields_stream.pprint()

    # si considerano solo i messaggi di tipo "Server Status"
    server_status_stream = fields_stream.filter(server_status_filter)
    # server_status_stream.pprint()

    ip_stats_stream = server_status_stream.map(server_status_mapper)
    # ip_stats_stream.pprint()

    ip_stats_stream = ip_stats_stream.reduceByKey(lambda a,b: [a[0]+b[0], a[1]+b[1]])
    # ip_stats_stream.pprint()

    ip_stats_stream = ip_stats_stream.map(lambda a: (a[0], a[1][0], a[1][1]))
    ip_stats_stream.pprint()
    ip_stats_stream.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()
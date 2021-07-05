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
        fields = line.strip().split(",")
        # "IP" not in line[2] perché potrebbero esserci anche pacchetti IPv6
        return "TCP" not in fields[2] and "IP" not in fields[2]

    def filter_field(line):
        fields = line.strip().split(",")
        words = fields[4].split(" ")
        # si levano le virgolette ""
        # return (words[2][1:-1], words[3][1:-1], words[4][1:-1], [words[6][1:-1], 1])
        return (words[0], 1)

    lines_stream = ssc.textFileStream(sys.argv[1])
    print(sys.argv[1])

    filtered_stream = lines_stream.filter(filter_line)
    clean_stream = filtered_stream.map(filter_field)
    agg_stream = clean_stream.reduceByKey(lambda a, b: a + b)
    
    agg_stream.pprint()
    agg_stream.foreachRDD(process)
    
    ssc.start()
    ssc.awaitTermination()
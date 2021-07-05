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
        
    def filter_field(line):
        words = line.strip().split(",")
        # si levano le virgolette ""
        # return (words[2][1:-1], words[3][1:-1], words[4][1:-1], [words[6][1:-1], 1])
        return (words[0], words[1], words[4])

    def filter_line(line): 
        words = line[2].split(" ")
        # "IP" not in line[2] perché potrebbero esserci anche pacchetti IPv6
        # return len(words) > 2 and line[2] != "TCP" and "IP" not in line[2]
        return words[0] == 'Dynamic' or words[0] == 'Zone'
        
    
    lines_stream = ssc.textFileStream(sys.argv[1])
    print(sys.argv[1])

    clean_stream = lines_stream.map(filter_field)
    filtered_stream = clean_stream.filter(filter_line)
    count_packet_1_stream = filtered_stream.map(lambda a: [(a[0], [1,0]), (a[1], [0,1])])
    count_packet_2_stream = count_packet_1_stream.flatMap(lambda a: a)
    count_packet_3_stream = count_packet_2_stream.reduceByKey(lambda a, b: [a[0] + b[0], a[1] + b[1]])

    count_packet_3_stream = count_packet_3_stream.map(lambda a: (a[0], a[1][0], a[1][1]))
    count_packet_3_stream.pprint()
    count_packet_3_stream.foreachRDD(process)
    
    ssc.start()
    ssc.awaitTermination()
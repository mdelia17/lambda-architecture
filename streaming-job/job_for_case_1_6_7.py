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
        return words[0] == 'Standard' or words[0] == 'DNS' or words[0] == 'Inverse'
        
    def build_certain(line):
        words = line[2].split(" ")
        if ((words[0] == 'Standard' or words[0] == 'Inverse') and words[2] != "response") or (words[0] == 'DNS' and words[4] != "response"):
            return (line[1], [0, 1])
        else:
            return (line[0], [1, 0])
    
    def build_uncertain(line):
        words = line[2].split(" ")
        if ((words[0] == 'Standard' or words[0] == 'Inverse') and words[2] != "response") or (words[0] == 'DNS' and words[4] != "response"):
            return (line[0], [1, 0])
        else:
            return (line[1], [0, 1])

    def agg(line):
        if line[1][1] != None:
            return (line[0], line[1][0][0] + line[1][1][0], line[1][0][1] + line[1][1][1])
        else: 
            packets_sent, packets_received = line[1][0] 
            return (line[0], packets_sent, packets_received)

    lines_stream = ssc.textFileStream(sys.argv[1])
    print(sys.argv[1])

    clean_stream = lines_stream.map(filter_field)
    filtered_stream = clean_stream.filter(filter_line)

    build_certain_stream = filtered_stream.map(build_certain)
    build_uncertain_stream = filtered_stream.map(build_uncertain)

    build_certain_agg_stream = build_certain_stream.reduceByKey(lambda a, b: [a[0] + b[0], a[1] + b[1]])
    build_uncertain_agg_stream =  build_uncertain_stream.reduceByKey(lambda a, b: [a[0] + b[0], a[1] + b[1]])

    join_stream = build_certain_agg_stream.leftOuterJoin(build_uncertain_agg_stream)

    final_stream = join_stream.map(agg)

    final_stream.pprint()
    final_stream.foreachRDD(process)
    
    ssc.start()
    ssc.awaitTermination()
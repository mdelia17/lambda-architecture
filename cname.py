import sys
import re

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *

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

if len(sys.argv) != 2:
    print("Usage: cname.py <input_directory>", file=sys.stderr)
    sys.exit(-1)

sc = SparkContext(appName="SparkStreamingApp")
ssc = StreamingContext(sc, 10)

def format_line(line):
    words = line.strip().split(",")
    print(line)
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

def process(time, rdd):
    # print("========= %s =========" % str(time))
    try: 
        # Get the singleton instance of SparkSession 
        spark = getSparkSessionInstance()
        # Convert to DataFrame
        columns = ["cname", "count"]
        df = rdd.toDF(columns)
        # df.printSchema()
        # df.show(truncate=False)

        df.write\
          .format("org.apache.spark.sql.cassandra")\
          .mode('append')\
          .options(keyspace="dns", table="cname")\
          .save()
    
        # spark.sql("SELECT * FROM mycatalog.dns.cname").show(truncate=False)
    except:
        pass

lines_stream = ssc.textFileStream(sys.argv[1])
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
# reduced_stream.pprint()
reduced_stream.foreachRDD(process)

ssc.start()
ssc.awaitTermination()
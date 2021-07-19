#!/usr/bin/env python3

"""spark application"""
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
import argparse
from pyspark.sql import SparkSession
import json

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

def prep(line):
    fields  = line.split(",")
    return [(fields[0], "H/NS"), (fields[1], "H/NS")]

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# initialize SparkSession with the proper configuration
spark = SparkSession \
    .builder \
    .appName("Spark Wordcount") \
    .getOrCreate()

# read the input file and obtain an RDD with a record for each line
lines_RDD = spark.sparkContext.textFile(input_filepath).cache()
lines_clean_RDD = lines_RDD.map(lambda a: json.loads(a.strip())["message"])

spark = getSparkSessionInstance()

df = spark.read.table("mycatalog.dns_streaming.nameservers")

from_df_to_RDD = df.rdd.map(list)
# print(from_df_to_RDD.collect())
ns_1_RDD = from_df_to_RDD.map(lambda a: (a[0], "NS"))
# print(ns_1_RDD.collect())

lst = ns_1_RDD.collect()
ns_RDD = spark.sparkContext.parallelize(lst)
# print(ns_RDD.collect())

prepare_1_RDD = lines_clean_RDD.flatMap(prep)
# print(prepare_1_RDD.collect())
ns_host_RDD = prepare_1_RDD.reduceByKey(lambda a, _: a)
# print(ns_host_RDD.collect())

find_host_RDD = ns_host_RDD.leftOuterJoin(ns_RDD)
# print(find_host_RDD.collect())
host_RDD = find_host_RDD.filter(lambda a: a[1][1] == None).map(lambda a: (a[0])).coalesce(1)
# print(host_RDD.collect())

# host_RDD.saveAsTextFile(output_filepath)

spark = getSparkSessionInstance()
columns = ["host"]
df = host_RDD.map(lambda x: (x, )).toDF(columns)
# df = host_RDD.toDF(columns)
df.show(truncate=False)

df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(keyspace="dns_batch", table="hosts")\
    .save()
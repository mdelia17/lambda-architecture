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
            .config("spark.cassandra.connection.host", "cas-node1")\
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

# df = spark.sql("SELECT * FROM mycatalog.dns.nameserver")
df = spark.read.table("mycatalog.dns.nameserver")

from_df_to_RDD = df.rdd.map(list)
ns_1_RDD = from_df_to_RDD.map(lambda a: (a[0], "NS"))

lst = ns_1_RDD.collect()
ns_RDD = spark.sparkContext.parallelize(lst)

prepare_1_RDD = lines_clean_RDD.flatMap(prep)
ns_host_RDD = prepare_1_RDD.reduceByKey(lambda a, _: a)

find_host_RDD = ns_host_RDD.leftOuterJoin(ns_RDD)
host_RDD = find_host_RDD.filter(lambda a: a[1][1] == None).map(lambda a: a[0]).coalesce(1)

host_RDD.saveAsTextFile(output_filepath)
#!/usr/bin/env python3
"""spark application"""

import json
import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output file path")
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

spark = SparkSession.builder.appName("Total and Average packet length").getOrCreate()

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

def parse_line(record):
    line = record.strip().split(",")
    return [line[0], line[1], line[2], int(line[3]), line[4]]

# funzione per filtrare le righe duplicate generate dal join con la coppia di indirizzi invertiti
# le righe di tipo (A,B) tali che esiste una riga (B,A) vengono filtrate e quindi viene mantenuta la riga con l'indirizzo A < B
# le righe di tipo (C,D) tali che non esiste una riga (D,C) vengono mantenute in quanto non esistono righe duplicate
def filter_double_pair(line):
    if line[1][1] == None:
        return True
    return min_addr_pair(line[0][0],(line[0][1]))

# funzione per determinare il minimo tra due indirizzi IP tramite il confronto di una porzione dell'indirizzo alla volta
def min_addr_pair(addr1, addr2):
    l1 = addr1.split(".")
    l2 = addr2.split(".")
    for i in range(len(l1)):
        if(int(l1[i]) < int(l2[i])):
            return True
        elif(int(l1[i]) > int(l2[i])):
            return False

# funzione che calcola il totale e la media dei byte scambiati tra ogni coppia di indirizzi
def total_avg_pair(line):
    if line[1][1] == None:
        return [line[0], line[1][0][0], line[1][0][0]/line[1][0][1], line[1][0][1]]
    return [line[0], line[1][0][0] + line[1][1][0], (line[1][0][0] + line[1][1][0]) / (line[1][0][1] + line[1][1][1]), line[1][0][1] + line[1][1][1]]

input_rdd = spark.sparkContext.textFile(input_filepath).cache()
# print(input_rdd.collect())

json_rdd = input_rdd.map(f=lambda line: json.loads(line.strip())["message"])
# print(json_rdd.collect())

lines_rdd = json_rdd.map(parse_line)
# print(lines_rdd.collect())

filtered_rdd = lines_rdd.filter(lambda line: line[2] == "DNS").map(lambda line:[(line[0], line[1]), [int(line[3]), 1]])
# print(filtered_rdd.collect())

reduced_rdd = filtered_rdd.reduceByKey(lambda a, b: [a[0]+b[0], a[1]+b[1]])
# print(reduced_rdd.collect())
# print(type(reduced_rdd))

inversed_reduced_rdd = reduced_rdd.map(lambda line: [(line[0][1], line[0][0]), [line[1][0], line[1][1]]])
# print(inversed_reduced_rdd.collect())
# print(type(inversed_reduced_rdd))

join_rdd = reduced_rdd.leftOuterJoin(inversed_reduced_rdd)
# print(join_rdd.collect())
# print(type(join_rdd))

output_rdd = join_rdd.filter(filter_double_pair).map(total_avg_pair)
# print(output_rdd.collect())

# sorted_rdd = output_rdd.sortBy(lambda line: line[1], ascending=False)
# print(sorted_rdd.collect())

final_rdd = output_rdd.map(lambda l: [l[0][0], l[0][1], l[1], l[2], l[3]])
# print(final_rdd.collect())

# final_rdd.coalesce(1,True).saveAsTextFile(output_filepath)

spark = getSparkSessionInstance()
columns = ["address_a", "address_b", "total", "avg", "count"]
# df = host_RDD.map(lambda x: (x, )).toDF(columns)
df = final_rdd.toDF(columns)
df.show(truncate=False)

df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(keyspace="dns_batch", table="total_avg_bytes")\
    .save()
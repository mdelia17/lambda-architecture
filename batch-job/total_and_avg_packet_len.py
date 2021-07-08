#!/usr/bin/env python3
"""spark application"""

import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output file path")
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

spark = SparkSession.builder.appName("Total and Average packet length").getOrCreate()

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
    return False

# funzione che calcola il totale e la media dei byte scambiati tra ogni coppia di indirizzi
def total_avg_pair(line):
    if line[1][1] == None:
        return [line[0], line[1][0][0], line[1][0][0]/line[1][0][1], line[1][0][1]]
    return [line[0], line[1][0][0] + line[1][1][0], (line[1][0][0] + line[1][1][0]) / (line[1][0][1] + line[1][1][1]), line[1][0][1] + line[1][1][1]]

input_rdd = spark.sparkContext.textFile(input_filepath).cache()
# print(input_rdd.collect())

lines_rdd = input_rdd.map(parse_line)
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

sorted_rdd = output_rdd.sortBy(lambda line: line[1], ascending=False)

sorted_rdd.coalesce(1,True).saveAsTextFile(output_filepath)
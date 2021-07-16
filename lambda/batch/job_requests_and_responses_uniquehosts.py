#!/usr/bin/env python3
"""spark application"""
import argparse

from pyspark.sql import SparkSession
import re
import json

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

spark = SparkSession \
    .builder \
    .appName("Esercizio-1") \
    .getOrCreate()

def format_line(line):
        words = line.strip().split(",")
        # print(line)
        return [words[0], words[1], words[4]]

def request(line):
        words = line[2].split(" ")
        # len(words) > 2 va fatto per colpa dell'header
        return words[0] == "Standard"

def line_lookup(line):
        info = line[2].strip().split(" ")
        if info[2] != "response":
                url = info[-1]
                domain = url.strip().split(".")
                if len(domain) > 1:
                        string = domain[-2] + "." + domain[-1]
                else:
                        string = domain[-1]
                return [[("A", string), {line[0]}]]
        else:
                l = []
                for i in range(len(info)-1):
                        if info[i] == "CNAME":
                                l.append([("CNAME", info[i+1]), {line[0]}])
                        if info[i] == "NS":
                                l.append([("NS", info[i+1]), {line[0]}])
        return l    

input_rdd = spark.sparkContext.textFile(input_filepath).cache()

json_rdd = input_rdd.map(f=lambda line: json.loads(line.strip())["message"])

fields_stream = json_rdd.map(format_line)

# si prendono solo i pacchetti DNS contenenti i CNAME
all_request_stream = fields_stream.filter(request)

# cambio del contenuto del campo info nel record
clear_request_stream = all_request_stream.map(line_lookup)

cnames_stream = clear_request_stream.flatMap(lambda line: line)

aggregate_stream = cnames_stream.reduceByKey(lambda a, b: a.union(b))

count_RDD = aggregate_stream.map(lambda line: (line[0], line[1], len(line[1])))

sorted_RDD = count_RDD.sortBy(lambda line: line[2], ascending=False)

sorted_RDD.coalesce(1,True).saveAsTextFile(output_filepath)                  

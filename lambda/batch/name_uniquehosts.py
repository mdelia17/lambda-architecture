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

def filter_field(line):
    words = line.strip().split(",")
    # si levano le virgolette ""
    # return [words[2][1:-1], words[3][1:-1], words[6][1:-1]]
    return (words[0], words[1], words[4])

def request(line): 
    words = line[2].split(" ")
    # len(words) > 2 va fatto per colpa dell'header
    return words[0] == "Standard" and words[2] != "response"

def reg_fun(line):
    # la parte iniziale è "Standard query 0xYYYY ...", non è interessante, si aggiunge " all'inizio per vedere una cosa più pulita, ma non sarebbe necessario, se non si aggiunge alla fine si deve mettere name[0... e non name[1...

    # questa serve se non si vuole far comparire "Standard query 0xYYYY ..." quando non c'è una richiesta di risolvere un nome come "google.com"
    # name = '"' + line[6][23:]
    name = line[2]
    # ci sono delle righe che contengono "Malformed Packet", vengono messe tutte insieme
    if "Malformed Packet" in name:
        return ("Malformed Packet", 1)
    else:
        # si cerca la parte di stringa che finisce con NOME.IT/COM/...
        clean_name = re.search("[^.]*[.][^.]*$", name)
        if clean_name != None:
            pair = clean_name.span()
            final_name =  name[pair[0]:pair[1]]
            # si prova a splittare la lista perché se si ha una stringa "A google.com", con il passaggio precedente viene restituito "A google.com", ma si vuole "google.com", se se riesce a splittare sulla base dello spazio si prende l'ultimo elemento della lista (che sarebbe "google.com")
            lst = re.split("\s", final_name)
            if len(lst) > 1:
                return (lst[-1], {line[0]})
            else: 
                return (final_name, {line[0]})
        else:
            return (name, {line[0]})

input_rdd = spark.sparkContext.textFile(input_filepath).cache()

json_rdd = input_rdd.map(f=lambda line: json.loads(line.strip())["message"])

fields_stream = json_rdd.map(filter_field)

# si prendono solo le richieste fatte al DNS e non le risposte
all_request_stream = fields_stream.filter(request)

clear_request_stream = all_request_stream.map(reg_fun)

aggregate_stream = clear_request_stream.reduceByKey(lambda a, b: a.union(b))

count_RDD = aggregate_stream.map(lambda line: (line[0], line[1], len(line[1])))

sorted_RDD = count_RDD.sortBy(lambda line: line[2], ascending=False)

sorted_RDD.coalesce(1,True).saveAsTextFile(output_filepath)     

import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, from_json
from pyspark.sql.types import *
from cassandra.cluster import Cluster

# initialize the SparkSession
spark = SparkSession \
    .builder \
    .appName("Structured Streaming application with Kafka") \
    .getOrCreate()
    # .config("spark.eventLog.enabled", "true")\
    # .config("spark.eventLog.dir", "file:///tmp/spark-events")\

spark.sparkContext.setLogLevel("ERROR")

lines_DF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "network-data") \
    .option("startingOffsets","latest")\
    .load()

def filter_field(line):
    words = line[0].strip().split(",")
    # si levano le virgolette ""
    # return (words[2][1:-1], words[3][1:-1], words[4][1:-1], [words[6][1:-1], 1])
    return (words[0], words[1], words[4])

def filter_1_6_7_line(line): 
    words = line[2].split(" ")
    # "IP" not in line[2] perché potrebbero esserci anche pacchetti IPv6
    # return len(words) > 2 and line[2] != "TCP" and "IP" not in line[2]
    return words[0] == 'Standard' or words[0] == 'DNS' or words[0] == 'Inverse'

def filter_3_4_line(line): 
    words = line[2].split(" ")
    # "IP" not in line[2] perché potrebbero esserci anche pacchetti IPv6
    # return len(words) > 2 and line[2] != "TCP" and "IP" not in line[2]
    return words[0] == 'Dynamic' or words[0] == 'Zone'

def filter_5_line(line):
    return line[2].startswith("Server status")

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
        return (line[0], [line[1][0][0] + line[1][1][0], line[1][0][1] + line[1][1][1]])
    else: 
        packets_sent, packets_received = line[1][0] 
        return (line[0], [packets_sent, packets_received])

def server_status_mapper(line):
    if line[2].startswith("Server status request response"):
        return (line[0], [1,0])
    else:
        return (line[1], [0,1])

def foreach_batch_function(df, epoch_id):
    # df.show(2, False)
    try:
        lines_stream = df.rdd.map(list) 

        clean_stream = lines_stream.map(filter_field)

        filtered_1_6_7_stream = clean_stream.filter(filter_1_6_7_line)
        filtered_3_4_stream = clean_stream.filter(filter_3_4_line)
        filtered_5_stream = clean_stream.filter(filter_5_line)

        # casi 1, 6 e 7 #
        build_certain_stream = filtered_1_6_7_stream.map(build_certain)
        build_uncertain_stream = filtered_1_6_7_stream.map(build_uncertain)
        build_certain_agg_stream = build_certain_stream.reduceByKey(lambda a, b: [a[0] + b[0], a[1] + b[1]])
        build_uncertain_agg_stream =  build_uncertain_stream.reduceByKey(lambda a, b: [a[0] + b[0], a[1] + b[1]])
        join_stream = build_certain_agg_stream.leftOuterJoin(build_uncertain_agg_stream)
        final_1_6_7_stream = join_stream.map(agg)
        # print(final_1_6_7_stream.collect())

        # casi 3 e 4 #
        count_packet_1_stream = filtered_3_4_stream.map(lambda a: [(a[0], [1,0]), (a[1], [0,1])])
        count_packet_2_stream = count_packet_1_stream.flatMap(lambda a: a)
        # count_packet_3_stream = count_packet_2_stream.reduceByKey(lambda a, b: [a[0] + b[0], a[1] + b[1]])
        # final_3_4_stream = count_packet_3_stream.map(lambda a: (a[0], a[1][0], a[1][1]))
        # print(count_packet_2_stream.collect())

        # caso 5 #
        ip_stats_stream = filtered_5_stream.map(server_status_mapper)
        # ip_stats_stream = ip_stats_stream.reduceByKey(lambda a,b: [a[0]+b[0], a[1]+b[1]])
        # final_5_stream = ip_stats_stream.map(lambda a: (a[0], a[1][0], a[1][1]))
        # print(ip_stats_stream.collect())

        final_stream = spark.sparkContext.union([final_1_6_7_stream, count_packet_2_stream, ip_stats_stream])

        final_stream = final_stream.reduceByKey(lambda a,b: [a[0] + b[0], a[1] + b[1]]).map(lambda l: (l[0], l[1][0], l[1][1]))
        
        hosts = ['cassandra-1']
        port = 9042

        # this object models a Cassandra cluster
        cluster = Cluster(contact_points=hosts, port=port)

        # initialize a session to interact with the cluster:
        session = cluster.connect(keyspace="dns_streaming",
                                wait_for_all_pools=True)
        for elem in final_stream.collect():
            # run a query
            session.execute("INSERT INTO dns_streaming.nameservers (address, packets_sent, packets_received) VALUES (%s, %s, %s)", (elem[0], elem[1], elem[2]))
    except:
        pass

schema = StructType() \
        .add("schema", StringType()) \
        .add("payload", StructType().add("message", StringType())) \
        

lines_DF = lines_DF\
    .selectExpr("cast(value as string)")\

lines_DF = lines_DF\
    .select(from_json(lines_DF.value, schema))\
    .select("from_json(value).payload.message")\
    .writeStream\
    .option("checkpointLocation", "file:///tmp/job_nameservers")\
    .foreachBatch(foreach_batch_function)\
    .start()  \
    .awaitTermination() 

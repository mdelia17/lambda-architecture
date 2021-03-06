import sys
from pyspark.streaming import StreamingContext
from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from cassandra.cluster import Cluster
from pyspark.sql.types import *

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

def filter_field(line):
    words = line[0].strip().split(",")
    # si levano le virgolette ""
    # return [words[2][1:-1], words[3][1:-1], words[6][1:-1]]
    return (words[0], words[1], words[4])

def request(line): 
    words = line[2].split(" ")
    # len(words) > 2 va fatto per colpa dell'header
    return words[0] == "Standard"

def reg_fun(line):
    info = line[2].strip().split(" ")
    if info[2] != "response":
        url = info[-1]
        domain = url.strip().split(".")
        if len(domain) > 1:
            string = domain[-2] + "." + domain[-1]
        else:
            string = domain[-1]
        return [[("A", string), 1]]
    else:
        l = []
        for i in range(len(info)-1):
            if info[i] == "CNAME":
                l.append([("CNAME", info[i+1]), 1])
            if info[i] == "NS":
                l.append([("NS", info[i+1]), 1])
    return l

def famous(line):
    name = line[0][1]
    number = line[1]
    d = {"search": ["google", "msn", "yahoo", "bing", "ucweb", "scorecardresearch"], "tech": ["apple", "cloud", "windows", "cloudfront", "amazonaws", "android", "ksmobile", "microsoft", "msftncsi", "flurry", "gmail", "appsflyer", "doubleclick", "truecaller"], "social": ["facebook", "whatsapp", "youtube", "skype", "twitter", "instagram"], "e-commerce": ["amazon"], "cdn": ["akamai", "fbcdn"], "danger": ["porn", "virus"]}
    for i in d:
        for x in d[i]:
            if x in name:
                return (i, number)
    return ("other", number)

def foreach_batch_function(df, epoch_id):
    try:
        lines_stream = df.rdd.map(list)
        fields_stream = lines_stream.map(filter_field)
        # si prendono solo le richieste fatte al DNS e non le risposte
        all_request_stream = fields_stream.filter(request)
        # si puliscono le richieste fatte e si mostra solo quale nome si vuole risolvere
        clear_request_stream = all_request_stream.map(reg_fun)
        flatten_stream = clear_request_stream.flatMap(lambda l: l)
        # si aggrega in modo da vedere per ogni nome quante richieste ci sono
        aggregate_stream = flatten_stream.reduceByKey(lambda a, b: a + b)
        # bisogna ordinare ogni batch, non ?? esiste una trasformazione "sortBy" su un DStream
        # si possono anche non ordinare tutti gli RDD del DStream, ma solo alcuni: https://stackoverflow.com/questions/39819126/spark-sort-dstream-by-key-and-limit-to-5-values
        # ordered_stream = aggregate_stream.transform(lambda rdd: rdd.sortBy(keyfunc=lambda a: a[1], ascending=False))
        # si mostra il totale delle richieste per i nomi pi?? famosi
        domain_stream = aggregate_stream.filter(lambda l: l[0][0] == "A")
        domain_clean_stream = aggregate_stream.map(lambda l: [l[0][1], l[0][0], l[1]])
        # print(domain_clean_stream.collect())
        famous_stream = domain_stream.map(famous)
        final_famous_stream = famous_stream.reduceByKey(lambda a, b: a + b)
        # print(final_famous_stream.collect())
        # Get the singleton instance of SparkSession
        
        for elem in final_famous_stream.collect():
            print(elem)
            # per ogni elem viene fatta la query e si ottengono tutte le righe che soddisfano la clausola where (?? al massimo una perch?? la query ?? fatta sulla chiave)
            address_lookup_stmt = session.prepare("SELECT requests FROM dns_streaming.searched_categories WHERE category=?")
            rows = session.execute(address_lookup_stmt, [elem[0]])
            # fa la insert dell'elem corrente, se gi?? esiste nel db viene sovrascritto
            session.execute("INSERT INTO dns_streaming.searched_categories (category, requests) VALUES (%s, %s)", (elem[0], int(elem[1])))
            # se l'elem stava nel db viene fatto un inserimento con i campi aggiornati
            for row in rows: 
                # print(row)
                new_requests = row.requests + int(elem[1])
                # print(new_count)
                session.execute("INSERT INTO dns_streaming.searched_categories (category, requests) VALUES (%s, %s)", (elem[0], new_requests))

        getSparkSessionInstance()
        # Get the singleton instance of SparkSession
        # spark = getSparkSessionInstance()
        # Convert to DataFrame
        columns = ["request_response", "type", "requests"]
        df = domain_clean_stream.toDF(columns)
        # df.printSchema()
        df.show(truncate=False)
        df.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(keyspace="dns_streaming", table="domain_requests_and_responses")\
            .save()
    except:
        pass

hosts = ['cassandra-1']
port = 9042

# this object models a Cassandra cluster
cluster = Cluster(contact_points=hosts, port=port)

# initialize a session to interact with the cluster:
session = cluster.connect(keyspace="dns_streaming",
                        wait_for_all_pools=True)

schema = StructType() \
        .add("schema", StringType()) \
        .add("payload", StructType().add("message", StringType())) \
            
lines_DF = lines_DF\
    .selectExpr("cast(value as string)")\

lines_DF = lines_DF\
    .select(from_json(lines_DF.value, schema))\
    .select("from_json(value).payload.message")\
    .writeStream\
    .option("checkpointLocation", "file:///tmp/job_requests_and_responses")\
    .foreachBatch(foreach_batch_function)\
    .start()  \
    .awaitTermination() 
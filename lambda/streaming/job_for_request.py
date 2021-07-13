import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, from_json
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
    .option("subscribe", "connect-file-pulse-quickstart-csv") \
    .option("startingOffsets","latest")\
    .load()

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

def filter_field(line):
    words = line[0].strip().split(",")
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
                return (lst[-1], 1)
            else: 
                return (final_name, 1)
        else:
            return (name, 1)

def famous(line):
    (name, number) = line
    if "google" in name:
        return ("google", number)
    else:
        if "youtube" in name:
            return ("youtube", number)
        else:
            if "facebook" in name:
                return ("facebook", number)
            else:
                if "instagram" in name:
                    return ("instagram", number)
                else:
                    if "akamai" in name:
                        return ("akamai", number)
                    else:
                        if "porn" in name:
                            return ("porn", number)
                        else:
                            if "amazon" in name:
                                return ("amazon", number)
                            else:
                                return ("altro", number)

def foreach_batch_function(df, epoch_id):
    # df.show(2, False)
    lines_stream = df.rdd.map(list)
    if not lines_stream.isEmpty():
        fields_stream = lines_stream.map(filter_field)

        # si prendono solo le richieste fatte al DNS e non le risposte
        all_request_stream = fields_stream.filter(request)

        # si puliscono le richieste fatte e si mostra solo quale nome si vuole risolvere
        clear_request_stream = all_request_stream.map(reg_fun)

        # si aggrega in modo da vedere per ogni nome quante richieste ci sono
        aggregate_stream = clear_request_stream.reduceByKey(lambda a, b: a + b)

        # bisogna ordinare ogni batch, non è esiste una trasformazione "sortBy" su un DStream
        # si possono anche non ordinare tutti gli RDD del DStream, ma solo alcuni: https://stackoverflow.com/questions/39819126/spark-sort-dstream-by-key-and-limit-to-5-values
        ordered_stream = aggregate_stream.transform(lambda rdd: rdd.sortBy(keyfunc=lambda a: a[1], ascending=False))

        # si mostra il totale delle richieste per i nomi più famosi
        famous_stream = ordered_stream.map(famous)
        final_famous_stream = famous_stream.reduceByKey(lambda a, b: a + b)

        if not final_famous_stream.isEmpty():
            # Get the singleton instance of SparkSession 
            spark = getSparkSessionInstance()
            # Convert to DataFrame
            columns = ["domain", "requests"]
            df = final_famous_stream.toDF(columns)
            df.printSchema()
            df.show(truncate=False)

            df.write\
                .format("org.apache.spark.sql.cassandra")\
                .mode('append')\
                .options(keyspace="dns", table="domain")\
                .save()

schema = StructType() \
        .add("schema", StringType()) \
        .add("payload", StructType().add("message", StringType())) \
            
lines_DF = lines_DF\
    .selectExpr("cast(value as string)")\

lines_DF = lines_DF\
    .select(from_json(lines_DF.value, schema))\
    .select("from_json(value).payload.message")\
    .writeStream\
    .option("checkpointLocation", "file:///tmp/job_for_request")\
    .foreachBatch(foreach_batch_function)\
    .start()  \
    .awaitTermination() 
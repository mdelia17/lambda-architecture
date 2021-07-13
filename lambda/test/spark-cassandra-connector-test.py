from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *

conf=SparkConf()
conf.set("spark.master","local[*]")
conf.set("spark.app.name","exampleApp")
sc= SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.appName("SQL Example").master("local[*]")\
    .config("spark.sql.catalog.mycatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")\
    .config("spark.cassandra.connection.host", "cas-node1")\
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")\
    .config("spark.cassandra.auth.username", "cassandra")\
    .config("spark.cassandra.auth.password", "cassandra")\
    .getOrCreate()

spark.sql("SELECT * FROM mycatalog.dns.test").show(truncate=False)

sc.stop()
quit()

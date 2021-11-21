from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.functions import col, row_number


#kafka variable
kafka_topic_name = "test-events"
kafka_bootstrap_servers = 'localhost:9092'

#spark session
spark = SparkSession \
        .builder \
        .appName("Structured Streaming ") \
        .master("local") \
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#spark read from kafka producer
lines = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "bigDataAssignment") \
  .load() \
  .selectExpr("timestamp", "CAST(value AS STRING) AS value")

#word count
words = lines.select(lines.timestamp, f.posexplode(split(lines.value, ':')))
filter_words = words.filter(words.pos==4)
filter_words = filter_words.drop('pos')
filter_words = filter_words.withColumnRenamed('col', 'word')

words_updated = filter_words.select(explode(split(filter_words.word, ' ')).alias('words'))

wordCounts = words_updated.groupBy(col('words')).count()


#Run the query that prints the running counts to the console
query = wordCounts.writeStream \
     .format('console') \
     .outputMode('update') \
     .option("truncate", "false") \
     .start()
              
query.awaitTermination()

# query_word = filter_words.writeStream \
#      .format('console') \
#      .outputMode('update') \
#      .option("truncate", "false") \
#      .start()
# query_word.awaitTermination()             


#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 KafkaConsumerSpark.py

from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.streaming.kafka import KafkaUtils

spark = SparkSession \
    .builder \
    .appName('TwitterStream') \
    .getOrCreate()
	
df = spark \
  .readStream \
  .format('kafka') \
  .option('kafka.bootstrap.servers', 'localhost:9092') \
  .option('subscribe', 'covid19') \
  .option('startingOffsets', 'latest') \
  .option('failOnDataLoss', 'false') \
  .load()

df = df.selectExpr('CAST(value AS STRING)', 'timestamp')

schema = StructType().add('created_at', StringType()).add('text', StringType()).add('user',StructType().add('screen_name', StringType())) 

df = df.select(from_json(col('value'), schema).alias('tweet'), 'timestamp')
df = df.select('tweet.created_at', 'tweet.user.screen_name', 'tweet.text')

# ---
# do some processing/aggregations here
# ---
			
query = df \
		.writeStream \
		.format('csv') \
		.option('path', 'C:/Users/Sujit/Documents/LearningSpark/csv') \
		.option('header', True) \
		.option('checkpointLocation', 'C:/Users/Sujit/Documents/LearningSpark/checkpoint') \
		.trigger(processingTime = '30 seconds') \
		.start()
		
query.awaitTermination()
# spark_structured_streaming_using_kafka_tweepy
Stream live tweets from Twitter using Tweepy into kafka which is then read by Spark Structured Streams and written out into a CSV

#### Steps to get going
1. Start Zookeeper
2. Start Kafka
3. Run the `kafka_producer_tweepy` notebook
4. Run the file `kafka_consumer_spark.py` to spark using `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 kafka_consumer_spark.py`

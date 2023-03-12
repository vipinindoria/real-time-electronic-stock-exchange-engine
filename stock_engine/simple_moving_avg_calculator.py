from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import from_json
from common import config_utils
from pyspark.sql.functions import col, explode, collect_list, window, udf
from datetime import datetime
from pyspark.sql.window import Window

cfg = config_utils.get_config('resources', 'app.yaml')

# define the duration of each batch in seconds
batch_duration = cfg.engine.kafka.batch_duration

# create a SparkSession object
scala_version = '2.12'
spark_version = '3.2.1'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1'
]
spark = SparkSession.builder.master("local").appName("SMA Calculation").config("spark.jars.packages", ",".join(packages)).getOrCreate()

# define the input stream
kafka_bootstrap_servers = cfg.enviornment.kafka["bootstrap.servers"]
sma_kafka_topic = cfg.engine.kafka.sma_topic
matched_kafka_topic = cfg.engine.kafka.matched_topic
kafka_access_key_id = cfg.enviornment.kafka["sasl.username"]
kafka_secret_access_key = cfg.enviornment.kafka["sasl.password"]
sma_kafka_consumer_group = cfg.engine.kafka.consumer["sma.group.id"]
kafka_sma_producer_checkpoint_dir = cfg.engine.kafka["sma.checkpoint.dir"]
kafka_security_protocol = "SASL_SSL"
kafka_sasl_mechanism = "PLAIN"
kafka_sasl_jaas_config = f"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_access_key_id}\" password=\"{kafka_secret_access_key}\";"

matched_order_stream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", matched_kafka_topic) \
  .option("kafka.security.protocol", kafka_security_protocol) \
  .option("kafka.sasl.mechanism", kafka_sasl_mechanism) \
  .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config) \
  .option("kafka.group.id", sma_kafka_consumer_group) \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss", "false") \
  .option("max.poll.interval.ms", 60000) \
  .load()


# define the schema of the input data
input_schema = StructType([StructField("buy_order_id", StringType(), True),
                           StructField("per_volume_buy_price", DoubleType(), True),
                           StructField("sell_order_id", StringType(), True),
                           StructField("per_volume_sell_price", DoubleType(), True),
                           StructField("trade_volume", IntegerType(), True),
                           StructField("execution_time", TimestampType(), True),
                           StructField("instrument", StringType(), True)])

# parse the incoming orders
parsed_orders = matched_order_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", input_schema).alias("data")).select("data.*")


# Define the window duration and sliding interval
watermarkDuration = "10 minutes"
windowDuration = "10 minutes"
slidingInterval = "5 minutes"

# Calculate the SMA closing price of each instrument in the sliding window
sma_df = parsed_orders \
    .groupBy("instrument", window("execution_time", windowDuration, slidingInterval)) \
    .agg(avg("per_volume_buy_price").alias("sma_price"))\
    .orderBy(["window.start", "sma_price"], ascending=[False, False])

result_df = sma_df.select("instrument", "sma_price", col("window.start").alias("w_start_time"), col("window.end").alias("w_end_time"))\
    .limit(4)


sma_query = result_df \
    .selectExpr("concat(CAST(instrument AS STRING), '_', CAST(w_start_time AS STRING), '_', CAST(w_end_time AS STRING)) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .outputMode("complete") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("kafka.security.protocol", kafka_security_protocol) \
    .option("kafka.sasl.mechanism", kafka_sasl_mechanism) \
    .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config) \
    .option("checkpointLocation", kafka_sma_producer_checkpoint_dir) \
    .option("topic", sma_kafka_topic) \
    .start()


# Output the results to the console
display_query = result_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

display_query.awaitTermination()
sma_query.awaitTermination()

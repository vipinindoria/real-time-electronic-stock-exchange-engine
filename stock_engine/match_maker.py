from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from common import config_utils
from pyspark.sql.functions import col, explode, collect_list, window, udf
from datetime import datetime

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
spark = SparkSession.builder.master("local").appName("Exchange Match Maker").config("spark.jars.packages", ",".join(packages)).getOrCreate()

# define the input stream
kafka_bootstrap_servers = cfg.enviornment.kafka["bootstrap.servers"]
kafka_topic = cfg.engine.kafka.topic
kafka_access_key_id = cfg.enviornment.kafka["sasl.username"]
kafka_secret_access_key = cfg.enviornment.kafka["sasl.password"]
kafka_consumer_group = cfg.engine.kafka.consumer["group.id"]
kafka_producer_checkpoint_dir = cfg.engine.kafka["checkpoint.dir"]
kafka_security_protocol = "SASL_SSL"
kafka_sasl_mechanism = "PLAIN"
kafka_sasl_jaas_config = f"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_access_key_id}\" password=\"{kafka_secret_access_key}\";"
input_stream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", kafka_topic) \
  .option("kafka.security.protocol", kafka_security_protocol) \
  .option("kafka.sasl.mechanism", kafka_sasl_mechanism) \
  .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config) \
  .option("kafka.group.id", kafka_consumer_group) \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss", "false") \
  .load()

# define the schema of the input data
input_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("instrument", StringType(), True),
    StructField("buy_sell", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("expiry", TimestampType(), True),
    StructField("order_time", TimestampType(), True),
    StructField("initial_order_time", TimestampType(), True)
])

# parse the incoming orders
parsed_orders = input_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", input_schema).alias("data")).select("data.*")

df = parsed_orders \
  .withWatermark("order_time", "10 minutes") \
  .groupBy(window("order_time", "10 minutes", "5 minutes"), "instrument") \
  .agg(collect_list(col("order_id")).alias("order_ids"), collect_list(col("price")).alias("prices"), \
       collect_list(col("volume")).alias("volumes"), collect_list(col("buy_sell")).alias("buy_sells"), \
       collect_list(col("expiry")).alias("expiries"), collect_list(col("initial_order_time")).alias("initial_order_times"))


# Define a function to match buy and sell orders
def match_orders(order_ids, prices, volumes, buy_sells, expiries, initial_order_times):
    order_ids = [x for x in order_ids]
    prices = [x for x in prices]
    volumes = [x for x in volumes]
    buy_sells = [x for x in buy_sells]
    expiries = [x for x in expiries]

    buy_orders = []
    sell_orders = []
    for i, buy_sell in enumerate(buy_sells):
        if buy_sell == "Buy" and expiries[i] > datetime.now():
            buy_orders.append((order_ids[i], prices[i], volumes[i], expiries[i], initial_order_times[i]))
        elif buy_sell == "Sell" and expiries[i] > datetime.now():
            sell_orders.append((order_ids[i], prices[i], volumes[i], expiries[i], initial_order_times[i]))
    trades = []
    remaining_trades = []
    remaining_sell_orders = []
    for buy_order in buy_orders:
        for sell_order in sell_orders:
            if buy_order[1] >= sell_order[1]:
                trade_volume = min(buy_order[2], sell_order[2])
                per_volume_buy_price = buy_order[1]/buy_order[2]
                per_volume_sell_price = sell_order[1] / sell_order[2]
                trades.append((buy_order[0], per_volume_buy_price, sell_order[0], per_volume_sell_price, trade_volume))
                buy_order = (buy_order[0], buy_order[1], buy_order[2] - trade_volume, buy_order[3], buy_order[4])
                sell_order = (sell_order[0], sell_order[1], sell_order[2] - trade_volume, sell_order[3], sell_order[4])
                remaining_sell_orders.append(sell_order)
                if buy_order[2] == 0:
                    break
        if buy_order[2] > 0:
            remaining_trades.append((buy_order[0], buy_order[2], buy_order[1], "Buy", datetime.now(), buy_order[4], buy_order[3]))
    for sell_order in remaining_sell_orders:
        if sell_order[2] > 0:
            remaining_trades.append((sell_order[0], sell_order[2], sell_order[1], "Sell", datetime.now(), sell_order[4], sell_order[3]))
    return trades, remaining_trades


# Define a UDF to apply the match_orders function to the grouped data
match_orders_udf = udf(
    lambda order_ids, prices, volumes, buy_sells, expiries, initial_order_times: match_orders(order_ids, prices, volumes, buy_sells, expiries, initial_order_times), \
    StructType([StructField("trades", ArrayType(StructType([StructField("buy_order_id", StringType(), True), \
                                                            StructField("per_volume_buy_price", DoubleType(), True), \
                                                            StructField("sell_order_id", StringType(), True), \
                                                            StructField("per_volume_sell_price", DoubleType(), True), \
                                                            StructField("trade_volume", IntegerType(), True)]))), \
                StructField("remaining_trades", ArrayType(StructType([StructField("order_id", StringType(), True), \
                                                                      StructField("volume", IntegerType(), True), \
                                                                      StructField("price", DoubleType(), True), \
                                                                      StructField("buy_sell", StringType(), True), \
                                                                      StructField("order_time", TimestampType(), True), \
                                                                      StructField("initial_order_time", TimestampType(), True), \
                                                                      StructField("expiry", TimestampType(), True)])))]))

# Apply the match_orders UDF to the grouped data
match_maker_df = df.select("*", match_orders_udf(col("order_ids"), col("prices"), col("volumes"), col("buy_sells"), col("expiries"), col("initial_order_times")).alias("matches"))

# Splitting the all_trades array column into matches_trades and remaining_trades
match_maker_df = match_maker_df.withColumn("matches_trades", col("matches.trades")) \
                       .withColumn("remaining_trades", col("matches.remaining_trades"))

# Creating a dataframe for matches trades
matched_df = match_maker_df.select("window.start", "window.end", "instrument", \
                               explode(col("matches_trades")).alias("trade")) \
                       .select("start", "end", "instrument", \
                               col("trade.buy_order_id"), col("trade.per_volume_buy_price"), col("trade.sell_order_id"), col("trade.per_volume_sell_price"), col("trade.trade_volume"))

# Creating a dataframe for remaining trades
remaining_df = match_maker_df.select("window.start", "window.end", "instrument", \
                                  explode(col("remaining_trades")).alias("trade")) \
                          .select("instrument", \
                                  col("trade.order_id").alias("order_id"), col("trade.volume").alias("volume"), \
                                  col("trade.price").alias("price"), col("trade.buy_sell").alias("buy_sell"), \
                                  col("trade.order_time").alias("order_time"), col("trade.initial_order_time").alias("initial_order_time"), \
                                  col("trade.expiry").alias("expiry"))

# Output the results to the console
match_query = matched_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

remaining_query = remaining_df \
    .selectExpr("concat(CAST(order_id AS STRING), '_', CAST(order_time AS STRING)) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("kafka.security.protocol", kafka_security_protocol) \
    .option("kafka.sasl.mechanism", kafka_sasl_mechanism) \
    .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config) \
    .option("checkpointLocation", kafka_producer_checkpoint_dir) \
    .option("topic", "ese-re-orders") \
    .start()

match_query.awaitTermination()
# Start the streaming query
remaining_query.awaitTermination()

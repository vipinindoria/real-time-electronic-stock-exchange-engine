from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, expr
from common import config_utils

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
  .load()

# define the schema of the input data
input_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("instrument_type", StringType(), True),
    StructField("order_type", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True)
])

# parse the incoming orders
parsed_orders = input_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", input_schema).alias("data")).select("data.*")

# group the orders by instrument type
orders_by_instrument = parsed_orders.groupBy("instrument_type")


def match_orders(key, values, state):
    # Extract the current state of the group
    count = state["count"]
    buy_orders = state["buy_orders"]

    # Convert the input values to a pandas dataframe for easier processing
    pdf = values.toPandas()

    # Filter the orders to only include sell orders
    sell_orders = pdf[pdf["order_type"] == "SELL"]

    # Iterate over the sell orders and try to match them with existing buy orders
    for i, sell_order in sell_orders.iterrows():
        # Find the first buy order with a matching price and quantity
        buy_order_index = -1
        for j, buy_order in enumerate(buy_orders):
            if buy_order["price"] == sell_order["price"] and buy_order["quantity"] == sell_order["quantity"]:
                buy_order_index = j
                break

        # If a matching buy order is found, generate a trade and remove the buy order from the state
        if buy_order_index >= 0:
            trade = {
                "timestamp": expr("current_timestamp()"),
                "instrument_type": key,
                "buy_order_id": buy_orders[buy_order_index]["order_id"],
                "sell_order_id": sell_order["order_id"],
                "price": sell_order["price"],
                "quantity": sell_order["quantity"]
            }
            buy_orders.pop(buy_order_index)
            count += 1

            # Emit the trade as an output row
            yield trade

    # Add the remaining buy orders to the state
    buy_orders += pdf[pdf["order_type"] == "BUY"].to_dict("records")

    # Update the state and emit a state output row
    new_state = {
        "count": count,
        "buy_orders": buy_orders
    }
    yield new_state

# Define the schema for the state data
state_schema = StructType([
    StructField("count", IntegerType()),
    StructField("buy_orders", ArrayType(StructType([
        StructField("order_id", StringType()),
        StructField("order_type", StringType()),
        StructField("price", DoubleType()),
        StructField("quantity", DoubleType())
    ])))
])

# Apply the mapGroupsWithState method to perform stateful transformations
trades = orders_by_instrument.mapGroupsWithState(
    mappingFunc=match_orders,
    outputMode="update",
    timeoutTimestamp=expr("current_timestamp()"),
    timeoutDuration=batch_duration,
    keyColumns=["instrument_type"],
    valueColumns=["order_id", "order_type", "price", "quantity"],
    stateSchema=state_schema,
    initialState=struct(lit(0).alias("count"), array().alias("buy_orders"))
)


# call the match_orders function and display results on console
output_stream = trades.selectExpr("to_json(struct(*)) AS value")
query = output_stream.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()

# wait for the stream to finish
query.awaitTermination()
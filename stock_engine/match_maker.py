from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# define the duration of each batch in seconds
batch_duration = 5

# create a Spark configuration object and a Spark context
conf = SparkConf().setAppName('Exchange Match Maker')
sc = SparkContext(conf=conf)

# create a StreamingContext with the batch duration
ssc = StreamingContext(sc, batch_duration)

# define the input stream
kafka_params = {'bootstrap.servers': 'localhost:9092'}
input_topic = 'my_orders_topic'
input_stream = KafkaUtils.createDirectStream(ssc, [input_topic], kafka_params)


# define the function that processes each batch
def process_batch(rdd):
    # parse the incoming orders
    orders = rdd.map(lambda x: x[1].split(',')).map(lambda x: (x[0], x[1], x[2], float(x[3]), int(x[4])))

    # group the orders by instrument type
    orders_by_instrument = orders.groupBy(lambda x: x[1])

    # match the buy and sell orders for each instrument
    trades = orders_by_instrument.flatMapValues(lambda x: sorted(x, key=lambda y: y[3])). \
        map(lambda x: (x[0], x[1][0], 'matched') if x[1][0][2] == 'buy' and x[1][-1][2] == 'sell' and x[1][0][3] >= x[1][-1][3] else (x[0], x[1][0], 'unmatched')). \
        filter(lambda x: x[2] == 'matched'). \
        zipWithIndex(). \
        map(lambda x: (x[0][0], x[0][1], x[1]))

    # print the trades
    trades.pprint()

# apply the processing function to each batch in the input stream
input_stream.foreachRDD(process_batch)

# start the StreamingContext
ssc.start()
ssc.awaitTermination()
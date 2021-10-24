import findspark
findspark.init('/Users/pravashpanigrahi/spark-3.0.2-bin-hadoop2.7')

import os
os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
# from pyspark_project.spark_streaming_ex.userdefined_KafkaUtils import KafkaUtils
from pyspark.streaming.kafka import KafkaUtils


sc = SparkContext('local[*]', 'KafkaSparkDemo')

streamsc = StreamingContext(sc, 5)  # '5' indicates the the time interval

message = KafkaUtils.createDirectStream(streamsc, topics=['test_kafka'],
                                        kafkaParams={"metadata.broker.list": "localhost:9092"})

words = message.flatMap(lambda msg: msg.split(' '))

pairs = words.map(lambda word: (word, 1))

word_counts = pairs.reduceByKey(lambda x, y: x + y)

word_counts.pprint()

streamsc.start()
streamsc.awaitTermination()

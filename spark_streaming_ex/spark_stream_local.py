"""
In terminal
    1. activate conda environment (conda activate <env_name>)
    2. run jupyter - jupyter notebook
    3. you will redirected to jupyter notebook there copy paste below code
    4. Then new Terminal, and type:
                                nc -lk <port_number> (ex: nc -lk 2222)
    5. Now start the jupyter notebook
    6. For testing type any sentence in the new Terminal.

    To fix Kernel Error or broken Python in Jupyter Notebook
        python/python3 -m pip install ipykernel
        python/python3 -m ipykernel install —user
"""

import findspark
findspark.init('/Users/pravashpanigrahi/spark-3.0.2-bin-hadoop2.7')

import os
os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext('local[*]', 'NetworkWordCount')

streamsc = StreamingContext(sc, 5)  # '5' indicates the the time interval

lines = streamsc.socketTextStream('localhost', 2222)

words = lines.flatMap(lambda line: line.split(' '))

pairs = words.map(lambda word: (word, 1))

word_counts = pairs.reduceByKey(lambda x, y: x + y)

word_counts.pprint()

streamsc.start()

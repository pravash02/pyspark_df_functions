"""
Run below in console with avro dependency
spark-submit --packages org.apache.spark:spark-avro_2.12:3.0.2 read_write_files_partitioned_dirs/read_file_from_partitioned_dirs.py

First read a avro file and create partitioned folder using some of its
column. Then read the files from partitioned folders using those columns.
you have slected first partition folder then the sub-partition folders will be
added as columns in the dataframe
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master("local") \
    .appName("read_files_from_partitioned_dirs") \
    .config("spark.some.config.option", "some-value").getOrCreate()

""" read a avro file and write in partitioned folders"""
# df = spark.read \
#     .format('avro') \
#     .load('read_write_files_partitioned_dirs/userdata1.avro')
# df = df.limit(10)
# df.write.partitionBy("gender", "id") \
#         .format("avro").save("read_write_files_partitioned_dirs/userdata_partition")


""" read avro file from partitioned folders"""
df = spark.read \
    .format('avro') \
    .load('read_write_files_partitioned_dirs/userdata_partition') \
    .where("gender='Male'")

df.show()

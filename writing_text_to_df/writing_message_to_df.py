"""
Run below in console with avro dependency
spark-submit --packages org.apache.spark:spark-avro_2.12:3.0.2 writing_text_to_df/writing_message_to_df.py
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master("local") \
    .appName("write_text_message_in_df") \
    .config("spark.some.config.option", "some-value").getOrCreate()

reference_dataframe = [
    ('TRANS', 'COMPLETED', 10, 10000.00, 10000.00), ('ACCT_DT_REJ', 'COMPLETED', -2, -50.00, -50.00),
    ('DPSL_REJECTED', 'COMPLETED', -1, 0.00, -100.00), ('RECYLOUT', 'COMPLETED', 1, 200.00, 0.00),
    ('SUSPREV', 'COMPLETED', 1, 0.00, 200.00), ('LEBAL', 'COMPLETED', 1, 0.00, 100.00),
    ('ETP', 'COMPLETED', 4, 20.00, 20.00), ('SUBLEGAL', 'COMPLETED', 2, 10.00, 10.00),
    ('REV', 'COMPLETED', 2, 25.00, 25.00), ('MPBAL', 'COMPLETED', 2, 15.00, 15.00),
    ('UK', 'COMPLETED', -2, -10.00, -10.00)
]
df = spark.createDataFrame(data=reference_dataframe,
                           schema=['step_name', 'status', 'count', 'debit', 'credit'])

tot_rec = 10
tot_failed = 5
stat = 'PASS'
message = f"""
              1) total records: {tot_rec}
              2) total failed: {tot_failed}
              3) overall status: {stat} 
              ************
              Thanks...
              ************
            """

""" for creating/writing avro file """
# res_dataframe = [(1, 1, 1, 'test1'), (1, 2, 1, 'test2'), (2, 1, 1, 'test3')]
# res_df = spark.createDataFrame(data=res_dataframe,
#                                schema=['occ_id', 'ccy_id', 'run_id', 'test'])
# res_df.write.partitionBy("occ_id", "ccy_id", "run_id") \
#         .format("avro").mode('append').save("writing_text_to_df/landing_partition")


""" read avro file from landing partitioned folders"""
df1 = spark.read \
    .format('avro') \
    .load('writing_text_to_df/landing_partition') \
    .where("occ_id='1' and ccy_id='1' and run_id='1'")
res_df1 = df1.withColumn('result', lit(message)).drop('test')
df2 = spark.read \
    .format('avro') \
    .load('writing_text_to_df/landing_partition') \
    .where("occ_id='1' and ccy_id='2' and run_id='1'")
res_df2 = df2.withColumn('result', lit(message)).drop('test')

""" writing the text files in the outbound partitioned folders"""
res_df1.coalesce(1).write.format("text")\
    .partitionBy("occ_id", "ccy_id", "run_id")\
    .mode('append')\
    .save("writing_text_to_df/outbound_partition")
res_df2.coalesce(1).write.format("text")\
    .partitionBy("occ_id", "ccy_id", "run_id")\
    .mode('append')\
    .save("writing_text_to_df/outbound_partition")

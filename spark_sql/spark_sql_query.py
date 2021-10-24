"""
spark-submit spark_sql/spark_sql_query.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master("local") \
    .appName("write_text_message_in_df") \
    .config("spark.some.config.option", "some-value").getOrCreate()

res_dataframe = [(1, 1, 1, 'test1'), (1, 2, 1, 'test1'), (2, 1, 1, 'test3')]
res_df = spark.createDataFrame(data=res_dataframe,
                               schema=['occ_id', 'ccy_id', 'run_id', 'test'])
res_df.createOrReplaceTempView('res_DF')

dff = spark.sql("select COUNT(*) rec_count, CASE WHEN COUNT(*)=0 THEN 'FAIL' ELSE 'PASS' END AS stat FROM "
                "(select * "
                "from res_DF WHERE test='test1')")
print("dff - ", dff.show())
print(dff.collect()[0][1])


# reading parametrized sql file
data = ''
with open('spark_sql/01_sql.sql', encoding='utf-8') as filename:
    data = filename.read()

print(data)
df_1 = spark.sql(data.format(test1='test1'))
df_1.show()

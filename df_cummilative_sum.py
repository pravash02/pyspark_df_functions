from functools import reduce
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master("local") \
    .appName("TEST_DF") \
    .config("spark.some.config.option", "some-value").getOrCreate()


rdf = {}
reference_dataframe = [('TRANS', 'COMPLETED', 10, 10000.00, 10000.00), ('ACCT_DT_REJ', 'COMPLETED', -2, -50.00, -50.00),
                       ('DPSL_REJECTED', 'COMPLETED', -1, 0.00, -100.00), ('RECYLOUT', 'COMPLETED', 1, 200.00, 0.00),
                       ('SUSPREV', 'COMPLETED', 1, 0.00, 200.00), ('LEBAL', 'COMPLETED', 1, 0.00, 100.00),
                       ('ETP', 'COMPLETED', 4, 20.00, 20.00), ('SUBLEGAL', 'COMPLETED', 2, 10.00, 10.00),
                       ('REV', 'COMPLETED', 2, 25.00, 25.00), ('MPBAL', 'COMPLETED', 2, 15.00, 15.00),
                       ('UK', 'COMPLETED', -2, -10.00, -10.00)]
df = spark.createDataFrame(data=reference_dataframe,
                                 schema=['step_name', 'status', 'count', 'debit', 'credit'])

w = Window.orderBy('step_name').rangeBetween(Window.unboundedPreceding, 0)
fnl_df = df.withColumn("adj_count", sum(col('count')).over(w)) \
           .withColumn("adj_debit", sum(col('debit')).over(w)) \
           .withColumn("adj_credit", sum(col('credit')).over(w)) \
           .withColumn("adj_amount", abs(col('adj_credit')-col('adj_debit')))
fnl_df.show()

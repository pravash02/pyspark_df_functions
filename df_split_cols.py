from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master("local") \
    .appName("TEST_DF") \
    .config("spark.some.config.option", "some-value").getOrCreate()


rdf = {}
reference_dataframe = [('TRANS-1', 'COMPLETED', 10, 10000.00, 10000.00), ('ACCT_DT_REJ-2', 'COMPLETED', -2, -50.00, -50.00),
                       ('DPSL_REJECTED-3', 'COMPLETED', -1, 0.00, -100.00), ('RECYLOUT-4', 'COMPLETED', 1, 200.00, 0.00),
                       ('SUSPREV-5', 'COMPLETED', 1, 0.00, 200.00), ('LEBAL-6', 'COMPLETED', 1, 0.00, 100.00),
                       ('ETP-7', 'COMPLETED', 4, 20.00, 20.00), ('SUBLEGAL-8', 'COMPLETED', 2, 10.00, 10.00),
                       ('REV-9', 'COMPLETED', 2, 25.00, 25.00), ('MPBAL-10', 'COMPLETED', 2, 15.00, 15.00),
                       ('UK-11', 'COMPLETED', -2, -10.00, -10.00)]
df = spark.createDataFrame(data=reference_dataframe,
                           schema=['step_col', 'status', 'count', 'debit', 'credit'])

split_col_df = split(df['step_col'], '-')
fnl_df = df.withColumn('step_name', split_col_df.getItem(0)) \
           .withColumn('step_id', split_col_df.getItem(1))

fnl_df.show()

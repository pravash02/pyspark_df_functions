from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.context import SparkContext

spark = SparkSession.builder \
    .master("local") \
    .appName("TEST_DF") \
    .config("spark.some.config.option", "some-value").getOrCreate()

rdf = {}
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
# df.show()
rdd1 = df.rdd

# df = df.withColumn('id', explode(array((0, 3))))
# print(df.show())

df1_ref = [
    ('Company1', '3D'), ('Company2', 'Accounting'), ('Company3', 'Wireless')]
df1 = spark.createDataFrame(data=df1_ref, schema=['Name', 'Sector'])
# df1.show()

df2_ref = [
        ('3D', 0, 0, 0, 0, 1, 0), ('Accounting', 0, 0, 0, 0, 0, 1),
        ('Wireless', 0, 0, 1, 0, 0, 0)]
df2 = spark.createDataFrame(data=df2_ref, schema=['Name', 'Automotive&Sports',
                                                              'Cleantech', 'Entertainment',
                                                              'Health', 'Manufacturing',
                                                              'Finance'])
# df2.show()

df3 = df1.join(df2, df1.Sector == df2.Name, 'inner') \
    .drop(df2.Name)

for col_name in df3.columns:
    if (df3.filter(col(col_name) == 0).count() == df3.select(col(col_name)).count()):
        df3 = df3.drop(col_name)

df3.show()

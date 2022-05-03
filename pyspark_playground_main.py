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

# df3.show()

data1 = [("Jhon", ["USA", "MX", "USW", "UK"], {'23': 'USA', '34': 'IND', '56': 'RSA'}),
         ("Joe", ["IND", "AF", "YR", "QW"], {'23': 'USA', '34': 'IND', '56': 'RSA'})]
data_frame = spark.createDataFrame(data=data1, schema=['name', 'subjectandID'])

df2 = data_frame.select(data_frame.name, explode(data_frame.subjectandID))


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

data1 = (("Bob", "IT", 4500), ("Maria", "IT", 4600), ("Maria", "HR", 4500),
         ("James", "IT", 4500), ("Sam", "HR", 3300), ("Jen", "HR", 3900),
         ("Jeff", "Marketing", 4500), ("Anand", "Marketing", 2000))
col = ["Name", "MBA_Stream", "SEM_MARKS"]
b = spark.createDataFrame(data1, col)
w = Window.partitionBy("MBA_Stream").orderBy("Name")
b.withColumn("Windowfunc_row", row_number().over(w))

from pyspark.sql.functions import rank

b.withColumn("Window_Rank", rank().over(w))

data1 = [{'Name': 'Jhon', 'Sal': 25000, 'Add': 'USA'}, {'Name': 'Joe', 'Sal': 30000, 'Add': 'USA'},
         {'Name': 'Tina', 'Sal': 22000, 'Add': 'IND'}, {'Name': 'Jhon', 'Sal': 15000, 'Add': 'USA'}]
a = spark.sparkContext.parallelize(data1)
b = a.toDF()
# b.sort("Name", "Sal").show()
# b.sort(col('Name').desc(), c
# ol('Sal').desc())

dfwithmax1 = b.groupBy("Add").agg(
    max("Sal").alias("salary"),
        first("Name").alias("employee_name"))
# dfwithmax1.show()

df_col1 = dfwithmax1.repartition(2)
# print(df_col1.rdd.glom().collect())
# print(df_col1.rdd.getNumPartitions())
df_col = dfwithmax1.coalesce(2)
# print(df_col.rdd.glom().collect())
# print(df_col.rdd.getNumPartitions())



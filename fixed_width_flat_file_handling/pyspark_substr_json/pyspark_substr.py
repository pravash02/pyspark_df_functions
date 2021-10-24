from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import substring


spark = SparkSession.builder \
    .master("local") \
    .appName("read_json_data_using_substr") \
    .config("spark.some.config.option", "some-value").getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)

df = spark.read.text("pyspark_project/fixed-width_flat_file_handling/pyspark_substr_json/substr_test.txt")
df.select(
    df.value.substr(1, 3).alias('id'),
    df.value.substr(4, 8).alias('date'),
    df.value.substr(12, 3).alias('name'),
    df.value.substr(15, 5).cast('salary').alias('integer')
).show()



### Without hardcoding
SchemaFile = spark.read\
    .format("json")\
    .option("header", "true")\
    .json('"pyspark_project/fixed-width_flat_file_handling/pyspark_substr_json/substr_test.json')

sfDict = map(lambda x: x.asDict(), SchemaFile.collect())

File = spark.read\
    .format("csv")\
    .option("header", "false")\
    .load("C:\Temp\samplefile.txt")

File.select(
    *[
        substring(
            str='_c0',
            pos=int(row['From']),
            len=int(row['To'])
        ).alias(row['Column'])
        for row in sfDict
    ]
).show()

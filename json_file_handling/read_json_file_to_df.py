# spark-submit json_file_handling/read_json_file_to_df.py

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = SparkSession.builder \
    .master("local") \
    .appName("read_json_file_df") \
    .config("spark.some.config.option", "some-value").getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)

### Reading json file
json_data = sqlContext.read.option("multiline", "true") \
    .json('json_file_handling/json_ex.json')
json_data.printSchema()
print(json_data.schema.fields)
complex_fields = dict([(field.name, field.dataType)
                       for field in json_data.schema.fields
                       if isinstance(field.dataType, T.ArrayType) or
                       isinstance(field.dataType, T.StructType)])
print(complex_fields)
print(complex_fields.keys())
qualify = list(complex_fields.keys())[0] + '_'

# {'results': ArrayType(StructType(List(StructField(a,LongType,true),
#             StructField(b,LongType,true),StructField(c,StringType,true))),true)}
# print(complex_fields)


df = json_data
flag = False
while len(complex_fields) != 0:
    col_name = list(complex_fields.keys())[0]

    if isinstance(complex_fields[col_name], T.StructType):
        expanded = [F.col(col_name + '.' + k).alias(col_name + '_' + k)
                    for k in [n.name for n in complex_fields[col_name]]]
        df = df.select("*", *expanded).drop(col_name)

    elif isinstance(complex_fields[col_name], T.ArrayType):
        df = df.withColumn(col_name, F.explode_outer(col_name))

    # checking NullType elements
    for col_name, type in df.dtypes:
        if type == 'null' and col_name == 'col_name':
            print("No nested columns found for 'col_name'")
            flag = True

    complex_fields = dict([(field.name, field.dataType)
                           for field in df.schema.fields
                           if isinstance(field.dataType, T.ArrayType) or
                           isinstance(field.dataType, T.StructType)])

# for df_col_name in df.columns:
#     df = df.withColumnRenamed(df_col_name, df_col_name.replace(qualify, ""))
print(df.show())

### Reading json data from a variable
# json_data = '{"results":[{"a":1,"b":2,"c":"name"},{"a":2,"b":5,"c":"foo"}]}'
# json_rdd = sc.parallelize([json_data])
# df = spark.read.json(json_rdd)
# df =df.withColumn("results", F.explode(df.results)).select(
#                          F.col("results.a").alias("a"),
#                          F.col("results.b").alias("b"),
#                          F.col("results.c").alias("c") )
# df.show()

# spark-submit json_file_handling/handle_json_in_column.py

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = SparkSession.builder \
    .master("local") \
    .appName("handle_json_object_df_columns") \
    .config("spark.some.config.option", "some-value").getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)


### handling single json data
df = spark.createDataFrame(
     [(1, ["foo1", "bar1"], '''{"x": 1.0, "y": 3.0}'''),
      (2, ["foo2", "bar2"], '''{"x": 2.0, "y": 4.0}''')],
     ("id", "an_array", "a_map"))

df.select("id", "an_array", "a_map")
df.show()
df = df.withColumn("x", F.json_tuple(df.a_map, 'x')) \
        .withColumn("y", F.json_tuple(df.a_map, 'y'))
df.show(truncate=False)

### cross join
df1 = df.withColumnRenamed('id', 'id1')
df2 = df.withColumnRenamed('id', 'id2')
df3 = df1.crossJoin(df2)
df3.show()




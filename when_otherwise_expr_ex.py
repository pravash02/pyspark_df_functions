# spark-submit when_otherwise_expr_ex.py
from pyspark.sql.functions import when, col, expr
from pyspark.sql import SparkSession, SQLContext

spark = SparkSession.builder \
    .master("local") \
    .appName("TEST_DF") \
    .config("spark.some.config.option", "some-value").getOrCreate()

# when and otherwise
dataDF = spark.createDataFrame([(66, "a", "4"), (67, "a", "0"), (70, "b", "4"), (71, "d", "4")]) \
    .toDF("id", "code", "amt")
new_dataDF = dataDF.withColumn('new_column', when((col('code') == 'a') | (col('code') == 'd'), 'A')
                               .when((col('code') == 'b') & (col('amt') == '4'), 'B')
                               .otherwise('A1'))

# expr
data = [("James", "M"), ("Michael", "F"), ("Jen", "")]
columns = ["name", "gender"]
df = spark.createDataFrame(data=data, schema=columns)

# Using CASE WHEN similar to SQL.
df2 = df.withColumn("gender", expr("CASE WHEN gender = 'M' THEN 'Male' " +
                                   "WHEN gender = 'F' THEN 'Female' ELSE 'unknown' END"))

data = [("2019-01-23", 1), ("2019-06-24", 2), ("2019-09-20", 3)]
df = spark.createDataFrame(data).toDF("date", "increment")

# Add Month value from another column
df.select(df.date, df.increment,
          expr("add_months(date,increment)")
          .alias("inc_date"))

# filter
data = [(100, 2), (200, 3000), (500, 500)]
df = spark.createDataFrame(data).toDF("col1", "col2")
df.filter(expr("col1 == col2"))
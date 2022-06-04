from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder \
    .master("local") \
    .appName("TEST_DF") \
    .config("spark.some.config.option", "some-value").getOrCreate()


''' UPDATE '''

cols = ['b']
cdf = spark.createDataFrame([(1, 'old', 100, 10.5), (3, 'old', 200, 10.5),
                             (4, 'old', 300, 10.1)], ("id", "b", "c", "d"))
cdf_cols = cdf.columns
other_cols = list(filter(lambda a: a not in cols, cdf_cols))

cdf2 = spark.createDataFrame([(1, 'new'), (3, 'new'), (2, 'new')], ("id", "b"))

new_cdf = cdf.alias('a').join(cdf2.alias('b'), ['id'], how='full_outer')\
             .select(*other_cols,  *(coalesce('b.' + col, 'a.' + col).alias(col) for col in cols))
new_cdf.show()


##############################

''' INSERT '''
ext_df = spark.createDataFrame([(5, 'old', 100, 10.4)], ("id", "b", "c", "d"))
new_df = cdf.union(ext_df)
new_df.show()


##############################

''' DELETE '''
df2 = new_cdf.filter(col('d') != 10.1)
df2.show()

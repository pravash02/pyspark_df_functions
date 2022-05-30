# spark-submit interview_prblms/test_1.py


from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master("local") \
    .appName("TEST_DF") \
    .config("spark.some.config.option", "some-value").getOrCreate()

# reference dataframe
struct_dataframe = [('A', 100, 107),
                    ('B', 20, 25),
                    ('C', 30, 25),
                    ('A', 100, 26),
                    ('A', 100, 101),
                    ('C', 30, 100)]
df = spark.createDataFrame(data=struct_dataframe,
                           schema=['item', 'cost_price', 'sell_price'])

# question - 1
# add prefix to all the column values in a daraframe

# solution:
pref_df = df
for col_nm in df.columns:
    pref_df = pref_df.withColumn(col_nm, concat(lit('prefix_val_'), lit(col(col_nm))))

# question - 2
# add a new column showing profit and loss for the sell price
# Solution:
pref_df = df
pref_df = pref_df.withColumn('profit_status',
                             when(col('sell_price') > col('cost_price'), lit('profit')).otherwise(lit('loss')))

# question - 3
# count number of items and populate those counts in new column tp respective item names
# Solution:
pref_df = df
w = Window.partitionBy('item')
pref_df = pref_df.withColumn('item_count', count(col('item')).over(w))

# question - 4
# get the items based on the max sell price
# Solution:
pref_df = df
w = Window.orderBy('item', 'sell_price')
pref_df = pref_df.withColumn('rank', rank().over(w))

df.createOrReplaceTempView('df')
spark.sql('select item, sell_price, rank() over(order by(item, sell_price)) as rank_no '
          'from df').createOrReplaceTempView('temp_df')
spark.sql('select item, MAX(sell_price) as max_sell_price, MAX(rank_no) from temp_df group by item')

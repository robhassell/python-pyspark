# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


# %%
spark = SparkSession.builder.appName("Sparking Streaming DF").getOrCreate()
word = spark.readStream.text("data/streaming").groupBy("value").count()

# %%
word.writeStream.format("console").start()

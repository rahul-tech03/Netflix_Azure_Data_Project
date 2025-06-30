# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Data Transformation.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("delta")\
    .option("headers", True)\
    .option("inferSchema", True)\
    .load("abfss://bronze@rpnetflixdl.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Dealing With Nulls.

# COMMAND ----------

df = df.fillna({"duration_minutes":0, "duration_seasons": 1})

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn("duration_minutes", col("duration_minutes").cast(IntegerType()))\
    .withColumn("duration_seasons",col("duration_seasons").cast(IntegerType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn("short_title",split(col("title"), ":").getItem(0))
df.display()

# COMMAND ----------

df = df.withColumn("rating",split(col("rating"),"-")[0])
df.display()

# COMMAND ----------

df = df.withColumn("flag",when(col("type")=='Movie',1)\
                       .when(col("type")=='TV Show',2)\
                       .otherwise(lit(0)))
df.display()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df = df.withColumn("duration_ranking",dense_rank().over(Window.orderBy(col("duration_minutes").desc())))

# COMMAND ----------

count_type_df = df.groupBy("type").agg(count("*").alias("total_count"))
count_type_df.display()

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("path","abfss://silver@rpnetflixdl.dfs.core.windows.net/netflix_titles")\
    .save()

# COMMAND ----------


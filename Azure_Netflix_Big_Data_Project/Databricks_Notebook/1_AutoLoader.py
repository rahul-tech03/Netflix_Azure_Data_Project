# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental Loading Uisng Auto Loader.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA netflix_catlog.net_schema;

# COMMAND ----------

checkpoint_location = "abfss://silver_container_name@storage_name.dfs.core.windows.net/checkpoint"

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("cloudFiles.schemaLocation",checkpoint_location)\
  .load("abfss://bronze_layer@storage_account_name.dfs.core.windows.net" )

# COMMAND ----------

df.display()

# COMMAND ----------

df.writeStream \
  .format("delta") \
  .option("checkpointLocation", checkpoint_location) \
  .trigger(processingTime='10 seconds') \
  .start("abfss://bronze_layer@storage_account_name.dfs.core.windows.net/netflix_titles")


# COMMAND ----------


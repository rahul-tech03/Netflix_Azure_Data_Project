# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Notebook - Gold Layer.

# COMMAND ----------

lookup_rules = {
    "rule1" : "show_id is NOT NULL"
}

# COMMAND ----------

@dlt.table(
    name = "gold_netflixdirectors"
)
@dlt.expect_or_drop(lookup_rules)
def myfun():
    df = spark.readStream.format("delta").load("abfss://silver@rpnetflixdl.dfs.core.windows.net/netflix_directors")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixcast"
)
@dlt.expect_or_drop(lookup_rules)
def myfun():
    df = spark.readStreamformat("delta").load("abfss://silver@rpnetflixdl.dfs.core.windows.net/netflix_cast")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixcountries"
)
@dlt.expect_or_drop(lookup_rules)
def myfun():
    df = spark.readStreamformat("delta").load("abfss://silver@rpnetflixdl.dfs.core.windows.net/netflix_countries")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixcategory"
)
@dlt.expect_or_drop("rule1", "show_id is NOT NULL")
def myfun():
    df = spark.readStreamformat("delta").load("abfss://silver@rpnetflixdl.dfs.core.windows.net/netflix_category")
    return df

# COMMAND ----------

@dlt.table


def gold_stg_netflix():
    df = spark.readStreamformat("delta").load("abfss://silver@rpnetflixdl.dfs.core.windows.net/netflix_titles")
    return df

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------



# COMMAND ----------

@dlt.view

def gold_trans_netflixtitles():
    df = spark.readstream.table("LIVE.gold_stg_netflix")
    df = df.withColumn("flag",lit(1))
    return df

# COMMAND ----------

masterdata_rules = {
    "rule1" : "flag is NOT NULL",
    "rule2" : "show_id is NOT NULL"
}

# COMMAND ----------

@dlt.table

@dlt.expect_all_or_drop(masterdata_rules)
def gold_netflixtitles():
    df = spark.readstream.table("LIVE.gold_trans_netflixtitles")
    return df
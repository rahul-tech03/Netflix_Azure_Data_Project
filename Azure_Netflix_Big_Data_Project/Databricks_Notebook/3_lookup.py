# Databricks notebook source
# MAGIC %md
# MAGIC ### Array Parameter.

# COMMAND ----------

import json

# COMMAND ----------

files = [
    {"sourcefolder": "netflix_directors", "targetfolder": "netflix_directors"},
    {"sourcefolder": "netflix_cast", "targetfolder": "netflix_cast"},
    {"sourcefolder": "netflix_category", "targetfolder": "netflix_category"},
    {"sourcefolder": "netflix_countries", "targetfolder": "netflix_countries"},
    {"sourcefolder": "netflix_directors", "targetfolder": "netflix_directors"}
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Jon utility to create a array.

# COMMAND ----------

dbutils.jobs.taskValues.set(key='myarr', value=json.dumps(files))

# COMMAND ----------


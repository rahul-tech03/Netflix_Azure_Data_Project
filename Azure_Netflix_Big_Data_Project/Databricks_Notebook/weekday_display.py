# Databricks notebook source
val = dbutils.jobs.taskValues.get("WeekdayLookup","weekoutput")

# COMMAND ----------

print(val)
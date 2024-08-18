# Databricks notebook source
from pyspark.sql.functions import max

# COMMAND ----------

df = spark.read.table("default.tesla_stock_price")
display(df)


# COMMAND ----------

df.select(max("Adj Close"), max("Volume"))\
  .withColumnRenamed("max(Adj Close)", "Max_Close")\
  .withColumnRenamed("max(Volume)", "Max_Volume")\
  .show(truncate=False)

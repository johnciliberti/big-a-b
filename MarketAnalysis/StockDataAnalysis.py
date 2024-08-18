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

# COMMAND ----------
df.select("Date", "Adj Close", "Volume")\
  .where(df.volume > 150000000)\
  .write.option("header","true")\
         .mode('overwrite')\
         .cvs("dbfs:/FileStore/outputs/tesla_highvoldays.csv")

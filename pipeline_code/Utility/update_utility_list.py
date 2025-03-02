# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

#Utility company list of NY   --Can be put in a csv and kept at a ADLS location
utility_list = [
    (1, "utility1", 1),
    (2, "utility2", 1),
    (3, "utility3", 0),
    (4, "utility4", 0),
    (5, "utility5", 0)
]

columns = ["id", "utility_name", "active_status"]
df = spark.createDataFrame(utility_list, columns)

# COMMAND ----------

# Write DataFrame as Delta Table (for now in default)
df.write.format("delta").mode("overwrite").saveAsTable("utility_list")


# COMMAND ----------

df1 = spark.read.format("delta").table("utility_list")
df1.show()
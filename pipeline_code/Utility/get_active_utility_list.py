# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

#get the list of utility companies active
df = spark.read.format("delta").table("utility_list")
df=df.filter(df.active_status==1).select("utility_name")
utility_list=[row["utility_name"] for row in df.collect()]

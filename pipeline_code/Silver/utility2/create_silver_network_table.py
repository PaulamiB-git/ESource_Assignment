# Databricks notebook source
# MAGIC %md
# MAGIC # Create Silver Network Table for Utility 1
# MAGIC This notebook is called by **create_silver_tables** notebook for **utility1**.
# MAGIC This notebook reads the bronze table and aggregates the data and loads it to Silver layer table using UPSERT
# MAGIC
# MAGIC - **Source** - Bronze Utility1 Network Table (can mention ADLS/S3 location)
# MAGIC - **Destination** - Bronze Utility1 Planned Table  (can mention ADLS/S3 location)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ../../Utility/helper

# COMMAND ----------

# Access the parameters passed from the calling notebook
year = dbutils.widgets.get("year")
month = dbutils.widgets.get("month")
processed_timestamp = dbutils.widgets.get("processed_timestamp")

# COMMAND ----------

month= '2'
year= '2025'
processed_timestamp = '2025-02-28 14:51:28.221227'
print(month, year)

# COMMAND ----------

#ingest network data
table_name = "workspace.utility2.bronze_utility2_circuits"
df_network_bronze = read_data(table_name)


# COMMAND ----------

df_network_silver = df_network_bronze.drop("feeder_dg_connected_since_refresh")

# COMMAND ----------

print(df_network_silver.count())

# COMMAND ----------

table_name = "workspace.utility2.silver_utility2_circuits"   #needs to be parameterised based on ADLS location in actual
mode = "upsert"
key= "circuit_id"
write_upsert_data(df_network_bronze, table_name, mode, key)
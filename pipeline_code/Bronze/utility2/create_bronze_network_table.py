# Databricks notebook source
# MAGIC %md
# MAGIC # Create Bronze Network Table for Utility 2
# MAGIC This notebook is called by **create_bronze_tables** notebook for **utility2**.
# MAGIC This notebook reads the raw table and after some cleaning (renaming to standard naming convention, dropping duplicates , column trasformation, converting datatypes as applicable) and loading it to Bronze layer table using UPSERT
# MAGIC
# MAGIC - **Source** - Raw Utility2 Network Table (can mention ADLS/S3 location)
# MAGIC - **Destination** - Bronze Utility2 Network Table  (can mention ADLS/S3 location)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../../Utility/helper

# COMMAND ----------

# Access the parameters passed from the calling notebook and use this to access the specific ADLS location for reading and writing data
env = dbutils.widgets.get("env")
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
table_name = "workspace.utility2.raw_utility2_circuits"
df_network_raw = read_data(table_name,year, month)


# COMMAND ----------

df_bronze_network_data = df_network_raw.withColumnRenamed("Master_CDF","circuit_id")\
                                       .dropDuplicates(["circuit_id"])\
                                       .withColumn("hca_refresh_date", to_date(substring(col("hca_refresh_date"), 1, 10), "yyyy/MM/dd"))\
                                       .drop("year_of_data","month_of_data")

# COMMAND ----------

table_name = "workspace.utility2.bronze_utility2_circuits"
mode = "upsert"
key="project_id"
write_upsert_data(df_bronze_network_data, table_name, mode, key)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.utility2.bronze_utility2_circuits limit 10

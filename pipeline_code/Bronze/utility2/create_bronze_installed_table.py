# Databricks notebook source
# MAGIC %md
# MAGIC # Create Bronze Installed Table for Utility 2
# MAGIC This notebook is called by **create_bronze_tables** notebook for **utility2**.
# MAGIC This notebook reads the raw table and after some cleaning (renaming to standard naming convention, dropping duplicates , column trasformation, converting datatypes as applicable) and loading it to Bronze layer table using UPSERT
# MAGIC
# MAGIC - **Source** - Raw Utility2 Installed Table (can mention ADLS/S3 location)
# MAGIC - **Destination** - Bronze Utility2 Planned Table  (can mention ADLS/S3 location)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable 

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
table_name = "workspace.utility2.raw_utility2_install_der"
df_installed_raw = read_data(table_name,year, month)
print(df_installed_raw.count())


# COMMAND ----------

df_bronze_installed_data = df_installed_raw.withColumnRenamed("DER_ID","der_id")\
                                       .withColumnRenamed("SERVICE_STREET_ADDRESS","service_street_address")\
                                       .withColumnRenamed("DER_TYPE","der_type")\
                                       .withColumnRenamed("DER_NAMEPLATE_RATING","der_nameplate_rating")\
                                       .withColumnRenamed("DER_INTERCONNECTION_LOCATION","der_interconnection_location")\
                                       .withColumnRenamed("INTERCONNECTION_COST","interconnection_cost")\
                                       .drop("year_of_data","month_of_data")
#print(df_bronze_installed_data.count())

# COMMAND ----------

df_bronze_installed_data = df_bronze_installed_data.dropDuplicates(["der_id"])
#print(df_bronze_installed_data.count())

# COMMAND ----------

table_name = "workspace.utility2.bronze_utility2_install_der"   #needs to be parameterised based on ADLS location in actual
mode = "upsert"
key="der_id"
write_upsert_data(df_bronze_installed_data, table_name, mode, key)

# COMMAND ----------

#%sql
#select count(*) from workspace.utility2.bronze_utility2_install_der

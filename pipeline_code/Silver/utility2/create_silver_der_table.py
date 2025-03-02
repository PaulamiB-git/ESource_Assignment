# Databricks notebook source
# MAGIC %md
# MAGIC # Create Silver Der Table for Utility 2
# MAGIC This notebook is called by **create_silver_tables** notebook for **utility2**.
# MAGIC This notebook reads the bronze installed and planned tables and stacks the data and loads it to Silver layer table using UPSERT
# MAGIC
# MAGIC - **Source** - Silver Utility2 Installed & Planned Tables (can mention ADLS/S3 location)
# MAGIC - **Destination** - Silver Utility2 Der Table  (can mention ADLS/S3 location)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ../../Utility/helper

# COMMAND ----------

# MAGIC %run ../Utility/schema/gold_schema

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

#ingest installed data
table_name1 = "workspace.utility2.bronze_utility2_installed_der"
df_installed_bronze = read_data(table_name1)
df_installed_silver= df_installed_bronze.withColumn("inverter_nameplate_rating", lit(None))\
                                        .withColumn("der_status", lit('Installed'))\
                                        .withColumn("planned_installation_date", lit(None))\
                                        .withColumn("total_mw_for_substation", lit(None))\
                                        .withColumn("der_status_rationale", lit(None))\
                                        .withColumn("interconnection_queue_request_id", lit(None))\
                                        .withColumn("interconnection_queue_position", lit(None))

# COMMAND ----------

#ingest planned data
table_name2 = "workspace.utility2.bronze_utility2_planned_der"
df_planned_bronze = read_data(table_name2)
df_planned_silver= df_planned_bronze.withColumn("der_id", lit(None))\
                                        .withColumn("service_street_address", lit(None))\
                                        .withColumn("interconnection_cost", lit(None))

# COMMAND ----------

df_der_silver = spark.createDataFrame([], utility2_silver_der_schema) 
df_der_silver=df_installed_silver.union(df_planned_silver)

# COMMAND ----------

table_name = "workspace.utility2.silver_utility2_der"   #needs to be parameterised based on ADLS location in actual
mode = "upsert"
key= "project_id"
write_upsert_data(df_der_silver, table_name, mode, key)
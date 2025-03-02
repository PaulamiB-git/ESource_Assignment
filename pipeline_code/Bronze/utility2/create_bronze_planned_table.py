# Databricks notebook source
# MAGIC %md
# MAGIC # Create Bronze Planned Table for Utility 2
# MAGIC This notebook is called by **create_bronze_tables** notebook for **utility2**.
# MAGIC This notebook reads the raw table and after some cleaning (renaming to standard naming convention, dropping duplicates , column trasformation, converting datatypes as applicable) and loading it to Bronze layer table using UPSERT
# MAGIC
# MAGIC - **Source** - Raw Utility2 Planned Table (can mention ADLS/S3 location)
# MAGIC - **Destination** - Bronze Utility2 Planned Table  (can mention ADLS/S3 location)
# MAGIC

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

# month= '2'
# year= '2025'
# processed_timestamp = '2025-02-28 14:51:28.221227'
# print(month, year)

# COMMAND ----------

#ingest network data
table_name = "workspace.utility2.raw_utility2_planned_der"
df_planned_raw = read_data(table_name,year, month)
#print(df_planned_raw.count())


# COMMAND ----------

df_bronze_planned_data = df_planned_raw.withColumnRenamed('DER_TYPE','der_type')\
                                       .withColumnRenamed('DER_NAMEPLATE_RATING','der_nameplate_rating')\
                                       .withColumnRenamed('INVERTER_NAMEPLATE_RATING','inverter_nameplate_rating')\
                                       .withColumnRenamed('PLANNED_INSTALLATION_DATE','planned_installation_date')\
                                       .withColumnRenamed('DER_STATUS','der_status')\
                                       .withColumnRenamed('DER_STATUS_RATIONALE','der_status_rationale')\
                                       .withColumnRenamed('TOTAL_MW_FOR_SUBSTATION','total_mw_for_substation')\
                                       .withColumnRenamed('INTERCONNECTION_QUEUE_REQUEST_ID','interconnection_queue_request_id')\
                                       .withColumnRenamed('INTERCONNECTION_QUEUE_POSITION','interconnection_queue_position')\
                                       .withColumnRenamed('DER_INTERCONNECTION_LOCATION','der_interconnection_location')\
                                       .drop("year_of_data","month_of_data")

# COMMAND ----------

df_bronze_planned_data = df_bronze_planned_data.dropDuplicates(["interconnection_queue_request_id"])
print(df_bronze_planned_data.count())

# COMMAND ----------

table_name = "workspace.utility2.bronze_utility2_planned_der"   #needs to be parameterised based on ADLS location in actual
mode = "upsert"
key="interconnection_queue_request_id"
write_upsert_data(df_bronze_planned_data, table_name, mode, key)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from workspace.utility2.bronze_utility2_planned_der

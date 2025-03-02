# Databricks notebook source
# MAGIC %md
# MAGIC # Create Bronze Network Table for Utility 1
# MAGIC This notebook is called by **create_bronze_tables** notebook for **utility1**.
# MAGIC This notebook reads the raw table and after some cleaning (renaming to standard naming convention, dropping duplicates , column trasformation, converting datatypes as applicable) and loading it to Bronze layer. As no unique identifier could be identified in this data, so this data is preserved in its native form in Bronze layer as well (no upsert).
# MAGIC
# MAGIC - **Source** - Raw Utility1 Network Table (can mention ADLS/S3 location)
# MAGIC - **Destination** - Bronze Utility1 Network Table  (can mention ADLS/S3 location)

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
processed_timestamp = '2025-02-27T23:00:15.637+0000'
print(month, year)

# COMMAND ----------

#ingest network data
table_name = "workspace.utility1.raw_utility1_circuits"
df_network_raw = read_data(table_name,year, month)


# COMMAND ----------

df_bronze_network_data = df_network_raw.withColumnRenamed("Circuits_Phase3_CIRCUIT","circuits_phase3_circuit")\
                                       .withColumnRenamed("Circuits_Phase3_NUMPHASES","circuits_phase3_numphases")\
                                       .withColumnRenamed("Circuits_Phase3_OVERUNDER","circuits_phase3_overunder")\
                                       .withColumnRenamed("Circuits_Phase3_PHASE","circuits_phase3_phase")\
                                       .withColumnRenamed("NYHCPV_csv_NSECTION","nyhcpv_csv_nsection")\
                                       .withColumnRenamed("NYHCPV_csv_NFEEDER","nyhcpv_csv_nfeeder")\
                                       .withColumnRenamed("NYHCPV_csv_NVOLTAGE","nyhcpv_csv_nvoltage")\
                                       .withColumnRenamed("NYHCPV_csv_NMAXHC","nyhcpv_csv_nmaxhc")\
                                       .withColumnRenamed("NYHCPV_csv_NMAPCOLOR","nyhcpv_csv_nmapcolor")\
                                       .withColumnRenamed("NYHCPV_csv_FFEEDER","nyhcpv_csv_ffeeder")\
                                       .withColumnRenamed("NYHCPV_csv_FVOLTAGE","nyhcpv_csv_fvoltage")\
                                       .withColumnRenamed("NYHCPV_csv_FMAXHC","nyhcpv_csv_fmaxhc")\
                                       .withColumnRenamed("NYHCPV_csv_FMINHC","nyhcpv_csv_fminhc")\
                                       .withColumnRenamed("NYHCPV_csv_FHCADATE","nyhcpv_csv_fhcadate")\
                                       .withColumnRenamed("NYHCPV_csv_FNOTES","nyhcpv_csv_fnotes")\
                                       .withColumnRenamed("Shape_Length","shape_length")

# COMMAND ----------

df_bronze_network_data = df_bronze_network_data.dropDuplicates()

# COMMAND ----------

table_name = "workspace.utility1.bronze_utility1_circuits"   #needs to be parameterised based on ADLS location in actual
mode = "overwrite"
partition_cols = ["year_of_data","month_of_data"]
write_data(df_bronze_network_data, table_name, mode, partition_cols, year, month)
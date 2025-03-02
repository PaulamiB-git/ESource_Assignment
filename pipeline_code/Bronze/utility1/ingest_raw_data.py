# Databricks notebook source
# MAGIC %md
# MAGIC # Create Raw Tables for Utility 2
# MAGIC This notebook is called by **ingest_raw_tables** notebook for **utility1**.
# MAGIC This notebook reads the raw table and loads it to Raw layer (without any changes) to the specific Year and Month Partition.
# MAGIC This helps in preserving the data received from various Utility systems in its native form for referencing. Old datas can be easily archived/deleted by deleting/vacumming those partitions.
# MAGIC
# MAGIC - **Source** -  Need to mention ADLS/S3 location of the CSV file
# MAGIC - **Destination** - Raw Utility1 Tables  (can mention ADLS/S3 location)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ../../Utility/helper

# COMMAND ----------

# Access the parameters passed from the calling notebook and use this to access the specific ADLS location for reading and writing data
env = dbutils.widgets.get("env")
year = dbutils.widgets.get("year")
month = dbutils.widgets.get("month")
processed_timestamp = dbutils.widgets.get("processed_timestamp")

# COMMAND ----------

#df = spark.read.csv("dbfs:/FileStore/tables/my_data.csv", header=True, inferSchema=True)
#display(df)
#spark.conf.set(
#    "fs.azure.account.key.<storage_account>.blob.core.windows.net",
#    "<your_storage_key>")
#df = spark.read.csv("wasbs://<container>@<storage_account>.blob.core.windows.net/my_data.csv", header=True, inferSchema=True)

# COMMAND ----------

#ingest network data
df_network = spark.sql('Select * from workspace.default.utility_1_circuits')
df_network = df_network.withColumn("year_of_data",lit(year))\
                         .withColumn("month_of_data",lit(month))\
                             .withColumn("processed_timestamp",lit(processed_timestamp)) 
table_name = "workspace.utility1.raw_utility1_circuits"   #needs to be parameterised based on ADLS location in actual
mode = "overwrite"
partition_cols = ["year_of_data","month_of_data"]
write_data(df_network, table_name, mode, partition_cols, year, month)

# COMMAND ----------

#ingest installed data
df_installed = spark.sql('Select * from workspace.default.utility_1_install_der')
df_installed = df_installed.withColumn("year_of_data",lit(year)) \
                           .withColumn("month_of_data",lit(month))\
                             .withColumn("processed_timestamp",lit(processed_timestamp)) 
table_name = "workspace.utility1.raw_utility1_install_der"   #needs to be parameterised based on ADLS location in actual
mode = "overwrite"
partition_cols = ["year_of_data","month_of_data"]
write_data(df_installed, table_name, mode, partition_cols, year, month)

# COMMAND ----------

#ingest planned data
df_planned = spark.sql('Select * from workspace.default.utility_1_planned_der')
df_planned = df_planned.withColumn("year_of_data",lit(year)) \
                           .withColumn("month_of_data",lit(month))\
                             .withColumn("processed_timestamp",lit(processed_timestamp)) 
table_name = "workspace.utility1.raw_utility1_planned_der"   #needs to be parameterised based on ADLS location in actual
mode = "overwrite"
partition_cols = ["year_of_data","month_of_data"]
write_data(df_planned, table_name, mode, partition_cols, year, month)

# COMMAND ----------

#%sql
#select count(*) from workspace.utility1.raw_utility1_circuits
#select count(*) from workspace.utility1.raw_utility1_install_der
#select count(*) from workspace.utility1.raw_utility1_planned_der

# Databricks notebook source
# MAGIC %md
# MAGIC # Create Silver Network Table for Utility 1
# MAGIC This notebook is called by **create_silver_tables** notebook for **utility1**.
# MAGIC This notebook reads the bronze table and aggregates the data and loads it to Silver layer table using UPSERT
# MAGIC
# MAGIC - **Source** - Bronze Utility1 Network Table (can mention ADLS/S3 location)
# MAGIC - **Destination** - Bronze Utility1 Network Table  (can mention ADLS/S3 location)

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

month= '2'
year= '2025'
processed_timestamp = '2025-02-28 14:51:28.221227'
print(month, year)

# COMMAND ----------

#ingest network data
table_name = "workspace.utility1.bronze_utility1_circuits"
df_network_bronze = read_data(table_name)


# COMMAND ----------

# %sql
# Select  circuits_phase3_circuit,nyhcpv_csv_nsection, nyhcpv_csv_nmapcolor, count(*) from workspace.utility1.bronze_utility1_circuits group by circuits_phase3_circuit, nyhcpv_csv_nsection, nyhcpv_csv_nmapcolor having count(*)>1 order by circuits_phase3_circuit,nyhcpv_csv_nsection, nyhcpv_csv_nmapcolor

# COMMAND ----------

# %sql
# select  circuits_phase3_circuit,nyhcpv_csv_nsection, nyhcpv_csv_nmapcolor, nyhcpv_csv_fhcadate, nyhcpv_csv_fvoltage, nyhcpv_csv_fmaxhc , nyhcpv_csv_fminhc,shape_length, count(*) from workspace.utility1.bronze_utility1_circuits  group by circuits_phase3_circuit, nyhcpv_csv_nsection, nyhcpv_csv_nmapcolor, nyhcpv_csv_fhcadate, nyhcpv_csv_fvoltage, nyhcpv_csv_fmaxhc , nyhcpv_csv_fminhc,shape_length having count(*)>1 order by circuits_phase3_circuit,nyhcpv_csv_nsection, nyhcpv_csv_nmapcolor, nyhcpv_csv_fhcadate, nyhcpv_csv_fvoltage, nyhcpv_csv_fmaxhc , nyhcpv_csv_fminhc,shape_length


# COMMAND ----------

# %sql
# select  circuits_phase3_circuit,nyhcpv_csv_ffeeder from workspace.utility1.bronze_utility1_circuits where circuits_phase3_circuit > nyhcpv_csv_ffeeder

# COMMAND ----------

# Perform groupBy and aggregate with avg
aggregated_df = df_network_bronze.groupBy("circuits_phase3_circuit","nyhcpv_csv_nsection","nyhcpv_csv_nmapcolor") \
                  .agg(
                      avg("nyhcpv_csv_fvoltage").alias("feeder_voltage"),
                      avg("nyhcpv_csv_fmaxhc").alias("feeder_max_hc"),
                      avg("nyhcpv_csv_fminhc").alias("feeder_min_hc"),
                      max("nyhcpv_csv_fhcadate").alias("hca_refresh_date"),
                      sum("shape_length").alias("shape_length")
                  )
aggregated_df = aggregated_df.withColumn("circuit_id",trim(concat(col("circuits_phase3_circuit"), 
                                                                       lit("_"), 
                                                                       coalesce(col("nyhcpv_csv_nsection").cast("string"), lit(" "))
                                                                    )).cast("string"))\
                            .withColumnRenamed("nyhcpv_csv_nmapcolor","color")\
                            .withColumn("processed_timestamp",lit(processed_timestamp)) \
                            .drop("circuits_phase3_circuit","nyhcpv_csv_nsection")

# COMMAND ----------

# aggregated_df.createOrReplaceTempView("silver_network")

# COMMAND ----------

# print(aggregated_df.count())

# COMMAND ----------

table_name = "workspace.utility1.silver_utility1_circuits"   #needs to be parameterised based on ADLS location in actual
mode = "upsert"
key= "circuit_id"
write_upsert_data(aggregated_df, table_name, mode, key)

# COMMAND ----------

# %sql
# select count(*),circuit_id from workspace.utility1.silver_utility1_circuits group by circuit_id having count(*)>1 order by circuit_id

# COMMAND ----------

# %sql
# select count(*) from workspace.utility1.silver_utility1_circuits limit 10

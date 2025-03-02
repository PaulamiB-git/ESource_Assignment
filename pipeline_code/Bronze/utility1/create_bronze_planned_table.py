# Databricks notebook source
# MAGIC %md
# MAGIC # Create Bronze Planned Table for Utility 1
# MAGIC This notebook is called by **create_bronze_tables** notebook for **utility1**.
# MAGIC This notebook reads the raw table and after some cleaning (renaming to standard naming convention, dropping duplicates , column trasformation, converting datatypes as applicable) and loading it to Bronze layer table using UPSERT
# MAGIC
# MAGIC - **Source** - Raw Utility1 Planned Table (can mention ADLS/S3 location)
# MAGIC - **Destination** - Bronze Utility1 Planned Table  (can mention ADLS/S3 location)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable 

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
table_name = "workspace.utility1.raw_utility1_planned_der" #can be parameterised
df_planned_raw = read_data(table_name,year, month)
#print(df_planned_raw.count())


# COMMAND ----------

df_bronze_planned_data = df_planned_raw.withColumnRenamed('ProjectType','project_type')\
                                       .withColumnRenamed('NamePlateRating','name_plate_rating')\
                                       .withColumnRenamed('InServiceDate','in_service_date')\
                                       .withColumnRenamed('ProjectStatus','project_status')\
                                       .withColumnRenamed('ProjectID','project_id')\
                                       .withColumnRenamed('CompletionDate','completion_date')\
                                       .withColumnRenamed('ProjectCircuitID','project_circuit_id')\
                                       .withColumnRenamed('Hybrid','hybrid')\
                                       .withColumnRenamed('SolarPV','solar_pv')\
                                       .withColumnRenamed('EnergyStorageSystem','energy_storage_system')\
                                       .withColumnRenamed('Wind','wind')\
                                       .withColumnRenamed('MicroTurbine','micro_turbine')\
                                       .withColumnRenamed('SynchronousGenerator','synchronous_generator')\
                                       .withColumnRenamed('InductionGenerator','induction_generator')\
                                       .withColumnRenamed('FarmWaste','farm_waste')\
                                       .withColumnRenamed('FuelCell','fuel_cell')\
                                       .withColumnRenamed('CombinedHeatandPower','combined_heat_and_power')\
                                       .withColumnRenamed('GasTurbine','gas_turbine')\
                                       .withColumnRenamed('Hydro','hydro')\
                                       .withColumnRenamed('InternalCombustionEngine','internal_combustion_engine')\
                                       .withColumnRenamed('SteamTurbine','steam_turbine')\
                                       .withColumnRenamed('Other','other')\
                                       .drop("year_of_data","month_of_data")

# COMMAND ----------

df_bronze_planned_data = df_bronze_planned_data.dropDuplicates(["project_id"])

# COMMAND ----------

table_name = "workspace.utility1.bronze_utility1_planned_der"   #needs to be parameterised based on ADLS location in actual
mode = "upsert"
key="project_id"
write_upsert_data(df_bronze_planned_data, table_name, mode, key)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from workspace.utility1.bronze_utility1_planned_der
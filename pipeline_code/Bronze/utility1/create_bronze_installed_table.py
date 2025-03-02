# Databricks notebook source
# MAGIC %md
# MAGIC # Create Bronze Installed Table for Utility 1
# MAGIC This notebook is called by **create_bronze_tables** notebook for **utility1**.
# MAGIC This notebook reads the raw table and after some cleaning (renaming to standard naming convention, dropping duplicates , column trasformation, converting datatypes as applicable) and loading it to Bronze layer table using UPSERT
# MAGIC
# MAGIC - **Source** - Raw Utility1 Installed Table (can mention ADLS/S3 location)
# MAGIC - **Destination** - Bronze Utility1 Installed Table  (can mention ADLS/S3 location)

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
table_name = "workspace.utility1.raw_utility1_install_der"
df_installed_raw = read_data(table_name,year, month)
print(df_installed_raw.count())


# COMMAND ----------

df_bronze_installed_data = df_installed_raw.withColumnRenamed("ProjectID","project_id")\
                                       .withColumnRenamed("ProjectType","project_type")\
                                       .withColumnRenamed("NamePlateRating","name_plate_rating")\
                                       .withColumnRenamed("TotalChargesCESIR","total_charge_cesir")\
                                       .withColumnRenamed("TotalChargesConstruction","total_charges_construction")\
                                       .withColumnRenamed("CESIR_EST","cesir_est")\
                                       .withColumnRenamed("SystemUpgrade_EST","system_upgrade_est")\
                                       .withColumnRenamed("ProjectCircuitID","project_circuit_id")\
                                       .withColumnRenamed("Hybrid","hybrid")\
                                       .withColumnRenamed("SolarPV","solar_pv")\
                                       .withColumnRenamed("EnergyStorageSystem","energy_storage_system")\
                                       .withColumnRenamed("Wind","wind")\
                                       .withColumnRenamed("MicroTurbine","micro_turbine")\
                                       .withColumnRenamed("SynchronousGenerator","synchronous_generator")\
                                       .withColumnRenamed("InductionGenerator","induction_generator")\
                                       .withColumnRenamed("FarmWaste","farm_waste")\
                                       .withColumnRenamed("FuelCell","fuel_cell")\
                                       .withColumnRenamed("CombinedHeatandPower","combined_heat_and_power")\
                                       .withColumnRenamed("GasTurbine","gasturbine")\
                                       .withColumnRenamed("Hydro","hydro")\
                                       .withColumnRenamed("InternalCombustionEngine","internal_combustion_engine")\
                                       .withColumnRenamed("SteamTurbine","steam_turbine")\
                                       .withColumnRenamed("Other","other")\
                                       .drop("year_of_data","month_of_data")

# COMMAND ----------

#drop duplicates based on project_id
df_bronze_installed_data = df_bronze_installed_data.dropDuplicates(["project_id"])

# COMMAND ----------

table_name = "workspace.utility1.bronze_utility1_install_der"   #needs to be parameterised based on ADLS location in actual
mode = "upsert"
key="project_id"
write_upsert_data(df_bronze_installed_data, table_name, mode, key)

# COMMAND ----------

#%sql
#Select count(*) from workspace.utility1.bronze_utility1_install_der

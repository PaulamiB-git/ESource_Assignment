# Databricks notebook source
# MAGIC %md
# MAGIC # Create Silver Der Table for Utility 1
# MAGIC This notebook is called by **create_silver_tables** notebook for **utility1**.
# MAGIC This notebook reads the bronze installed and planned tables and stacks the data and loads it to Silver layer table using UPSERT
# MAGIC
# MAGIC - **Source** - Silver Utility1 Installed & Planned Tables (can mention ADLS/S3 location)
# MAGIC - **Destination** - Silver Utility1 Der Table  (can mention ADLS/S3 location)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ../../Utility/helper

# COMMAND ----------

# MAGIC %run ../Utility/schema/gold_schema

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

#ingest installed data
table_name1 = "workspace.utility1.bronze_utility1_installed_der"
df_installed_bronze = read_data(table_name1)
df_installed_silver_interim = df_installed_bronze.withColumn("der_type", when(col("solar_pv") != 0, col("solar_pv"))
                                                                          .when(col("energy_storage_system") != 0, lit("energy_storage_system"))
                                                                          .when(col("wind") != 0, lit("wind"))
                                                                          .when(col("micro_turbine") != 0, lit("micro_turbine"))
                                                                          .when(col("synchronous_generator") != 0, lit("synchronous_generator"))
                                                                          .when(col("induction_generator") != 0, lit("induction_generator"))
                                                                         .when(col("farm_waste") != 0, lit("farm_waste"))
                                                                          .when(col("fuel_cell") != 0, lit("fuel_cell"))
                                                                          .when(col("combined_heat_and_power") != 0, lit("combined_heat_and_power"))
                                                                          .when(col("gasturbine") != 0, lit("gasturbine"))
                                                                          .when(col("hydro") != 0, lit("hydro"))
                                                                          .when(col("internal_combustion_engine") != 0, lit("internal_combustion_engine"))
                                                                          .when(col("steam_turbine") != 0, lit("steam_turbine"))
                                                                          .when(col("other") != 0, lit("other"))
                                                                          .otherwise(lit("unknown")))
df_planned_silver_interim = df_planned_interim.withColumn("der_count", when(col("solar_pv") != 0, col("solar_pv"))
                                                                          .when(col("energy_storage_system") != 0, col("energy_storage_system"))
                                                                          .when(col("wind") != 0, col("wind"))
                                                                          .when(col("micro_turbine") != 0, col("micro_turbine"))
                                                                          .when(col("synchronous_generator") != 0, col("synchronous_generator"))
                                                                          .when(col("induction_generator") != 0, col("induction_generator"))
                                                                          .when(col("farm_waste") != 0, col("farm_waste"))
                                                                          .when(col("fuel_cell") != 0, col("fuel_cell"))
                                                                          .when(col("combined_heat_and_power") != 0, col("combined_heat_and_power"))
                                                                          .when(col("gasturbine") != 0, col("gasturbine"))
                                                                          .when(col("hydro") != 0, col("hydro"))
                                                                          .when(col("internal_combustion_engine") != 0, col("internal_combustion_engine"))
                                                                          .when(col("steam_turbine") != 0, col("steam_turbine"))
                                                                          .when(col("other") != 0, col("other"))
                                                                          .otherwise(lit(None)))                                                                         
df_installed_silver= df_installed_silver_interim.withColumn("in_service_date", lit(None))\
                                        .withColumn("project_status", lit('Installed'))\
                                        .withColumn("completion_date", lit(None))\
                                        .drop("solar_pv","energy_storage_system","wind","micro_turbine","synchronous_generator","synchronous_generator","farm_waste","fuel_cell","combined_heat_and_power","gasturbine","hydro","internal_combustion_engine","steam_turbine","other")

# COMMAND ----------

#ingest planned data
table_name2 = "workspace.utility1.bronze_utility1_planned_der"
df_planned_bronze = read_data(table_name2)
df_planned_silver_interim = df_planned_bronze.withColumn("der_type", when(col("solar_pv") != 0, col("solar_pv"))
                                                                          .when(col("energy_storage_system") != 0, lit("energy_storage_system"))
                                                                          .when(col("wind") != 0, lit("wind"))
                                                                          .when(col("micro_turbine") != 0, lit("micro_turbine"))
                                                                          .when(col("synchronous_generator") != 0, lit("synchronous_generator"))
                                                                          .when(col("induction_generator") != 0, lit("induction_generator"))
                                                                         .when(col("farm_waste") != 0, lit("farm_waste"))
                                                                          .when(col("fuel_cell") != 0, lit("fuel_cell"))
                                                                          .when(col("combined_heat_and_power") != 0, lit("combined_heat_and_power"))
                                                                          .when(col("gasturbine") != 0, lit("gasturbine"))
                                                                          .when(col("hydro") != 0, lit("hydro"))
                                                                          .when(col("internal_combustion_engine") != 0, lit("internal_combustion_engine"))
                                                                          .when(col("steam_turbine") != 0, lit("steam_turbine"))
                                                                          .when(col("other") != 0, lit("other"))
                                                                          .otherwise(lit("unknown")))
df_planned_silver_interim = df_planned_interim.withColumn("der_count", when(col("solar_pv") != 0, col("solar_pv"))
                                                                          .when(col("energy_storage_system") != 0, col("energy_storage_system"))
                                                                          .when(col("wind") != 0, col("wind"))
                                                                          .when(col("micro_turbine") != 0, col("micro_turbine"))
                                                                          .when(col("synchronous_generator") != 0, col("synchronous_generator"))
                                                                          .when(col("induction_generator") != 0, col("induction_generator"))
                                                                          .when(col("farm_waste") != 0, col("farm_waste"))
                                                                          .when(col("fuel_cell") != 0, col("fuel_cell"))
                                                                          .when(col("combined_heat_and_power") != 0, col("combined_heat_and_power"))
                                                                          .when(col("gasturbine") != 0, col("gasturbine"))
                                                                          .when(col("hydro") != 0, col("hydro"))
                                                                          .when(col("internal_combustion_engine") != 0, col("internal_combustion_engine"))
                                                                          .when(col("steam_turbine") != 0, col("steam_turbine"))
                                                                          .when(col("other") != 0, col("other"))
                                                                          .otherwise(lit(None)))
df_planned_silver= df_planned_silver_interim.withColumn("total_charge_cesir", lit(None))\
                                        .withColumn("total_charges_construction", lit(None))\
                                        .withColumn("cesir_est", lit(None))\
                                        .withColumn("system_upgrade_est", lit(None))\
                                        .drop("solar_pv","energy_storage_system","wind","micro_turbine","synchronous_generator","synchronous_generator","farm_waste","fuel_cell","combined_heat_and_power","gasturbine","hydro","internal_combustion_engine","steam_turbine","other")
                                        .withColumn("project_status", coalesce(col("project_status"), lit("Planned")))\

# COMMAND ----------

df_der_silver = spark.createDataFrame([], silver_der_schema) 
df_der_silver=df_installed_silver.union(df_planned_silver)

# COMMAND ----------

table_name = "workspace.utility1.silver_utility1_der"   #needs to be parameterised based on ADLS location in actual
mode = "upsert"
key= "project_id"
write_upsert_data(df_der_silver, table_name, mode, key)

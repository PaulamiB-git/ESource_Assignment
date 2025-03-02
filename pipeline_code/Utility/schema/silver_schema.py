# Databricks notebook source
# MAGIC %md
# MAGIC #This notebook defines the schema and mappings applied to the data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, TimestampType

# COMMAND ----------

utility1_silver_der_schema_old = StructType([
   StructField("project_id", LongType(), True),
    StructField("project_type", StringType(), True),
    StructField("name_plate_rating", StringType(), True),
    StructField("total_charge_cesir", StringType(), True),
    StructField("total_charges_construction", StringType(), True),
    StructField("cesir_est", StringType(), True),
    StructField("system_upgrade_est", StringType(), True),
    StructField("project_circuit_id", StringType(), True),
    StructField("hybrid", StringType(), True),
    StructField("solar_pv", DoubleType(), True),
    StructField("energy_storage_system", DoubleType(), True),
    StructField("wind", DoubleType(), True),
    StructField("micro_turbine", LongType(), True),
    StructField("synchronous_generator", LongType(), True),
    StructField("induction_generator", LongType(), True),
    StructField("farm_waste", DoubleType(), True),
    StructField("fuel_cell", LongType(), True),
    StructField("combined_heat_and_power", DoubleType(), True),
    StructField("gasturbine", LongType(), True),
    StructField("hydro", LongType(), True),
    StructField("internal_combustion_engine", LongType(), True),
    StructField("steam_turbine", LongType(), True),
    StructField("other", LongType(), True),
    StructField("in_service_date", LongType(), True),
    StructField("project_status", LongType(), True),
    StructField("completion_date", LongType(), True),
    StructField("processed_timestamp", StringType(), True)
])

# COMMAND ----------

utility1_silver_der_schema = StructType([
   StructField("project_id", LongType(), True),
    StructField("project_type", StringType(), True),
    StructField("name_plate_rating", StringType(), True),
    StructField("total_charge_cesir", StringType(), True),
    StructField("total_charges_construction", StringType(), True),
    StructField("cesir_est", StringType(), True),
    StructField("system_upgrade_est", StringType(), True),
    StructField("project_circuit_id", StringType(), True),
    StructField("hybrid", StringType(), True),
    StructField("der_type", StringType(), True),
    StructField("der_count", StringType(), True),
    StructField("in_service_date", LongType(), True),
    StructField("project_status", LongType(), True),
    StructField("completion_date", LongType(), True),
    StructField("processed_timestamp", StringType(), True)
])

# COMMAND ----------

utility2_silver_der_schema = StructType([
    StructField("der_id", IntegerType(), True),
    StructField("service_street_address", StringType(), True),
    StructField("der_type", StringType(), True),
    StructField("interconnection_cost", DoubleType(), True),
    StructField("der_nameplate_rating", DoubleType(), True),
    StructField("inverter_nameplate_rating", StringType(), True),
    StructField("planned_installation_date", DateType(), True),
    StructField("der_status", StringType(), True),
    StructField("der_status_rationale", StringType(), True),
    StructField("total_mw_for_substation", DoubleType(), True),
    StructField("interconnection_queue_request_id", LongType(), True),
    StructField("interconnection_queue_position", StringType(), True),
    StructField("der_interconnection_location", StringType(), True),
    StructField("processed_timestamp", StringType(), True)
])
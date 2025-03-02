# Databricks notebook source
# MAGIC %md
# MAGIC #This notebook defines the schema and mappings applied to the data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, TimestampType

# COMMAND ----------

gold_network_schema = StructType([
    StructField("utility_name", StringType(), nullable=True),
    StructField("circuit_id", StringType(), nullable=True),
    StructField("color", StringType(), nullable=True),
    StructField("feeder_voltage", DoubleType(), nullable=True),
    StructField("feeder_max_hc", DoubleType(), nullable=True),
    StructField("feeder_min_hc", DoubleType(), nullable=True),
    StructField("hca_refresh_date", TimestampType(), nullable=True),
    StructField("shape_length", DoubleType(), nullable=True),
    StructField("processed_timestamp", StringType(), nullable=True)
])

# COMMAND ----------

gold_der_schema = StructType([
    StructField("utility_name", StringType(), nullable=True),
    StructField("der_type", StringType(), True),
    StructField("der_status", StringType(), True),
    StructField("total_count", DoubleType(), True),
    StructField("total_charges", DoubleType(), True),
    StructField("avg_nameplate_rating", DoubleType(), True),
    StructField("processed_timestamp", StringType(), True)
])
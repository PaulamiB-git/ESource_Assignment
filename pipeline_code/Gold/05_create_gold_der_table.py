# Databricks notebook source
# MAGIC %md
# MAGIC # Gold notebook - common for All Utlities

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime

# COMMAND ----------

# MAGIC %run ../Utility/helper

# COMMAND ----------

# MAGIC %run ../Utility/schema/gold_schema

# COMMAND ----------

#get the list of utility companies active
df = spark.read.format("delta").table("utility_list")
df=df.filter(df.active_status==1).select("utility_name")
utility_list=[row["utility_name"] for row in df.collect()]
print(utility_list)

# COMMAND ----------

#This needs to be parameter of the workflow
dbutils.widgets.multiselect(
    name='utility_name', 
    defaultValue='utility1',  # Default selected value as a string
    choices=utility_list,         # List of choices to display
    label='Utility Name'        # Label for the widget
)
utility_name = dbutils.widgets.get("utility_name")
selected_utilities = utility_name.split(',')

# COMMAND ----------

#print(utility_name)

# COMMAND ----------

#run the specific utility flow
# read job parameters
env = dbutils.widgets.get("env")
current_month = dbutils.widgets.get("month_of_run")
current_year = dbutils.widgets.get("year_of_run")
processed_timestamp = datetime.now()

params = {
    "env" : env,
    "year": current_year,
    "month": current_month,
    "processed_timestamp" : processed_timestamp
}


# COMMAND ----------

#ingest network data for each Utility and stack on top of each other
df_der_gold = spark.createDataFrame([], gold_der_schema) 
for utility in selected_utilities:  
    table_name = f"workspace.{utility}.silver_{utility}_der"
    df_der_gold_utility = read_silver_der_data(utility, table_name)
    df_der_gold_utility.withColumn("processed_timestamp",lit(processed_timestamp)) 
    df_der_gold = df_der_gold.union(df_der_gold_utility)
    print(df_der_gold.count())

# COMMAND ----------

table_name = "workspace.gold.gold_der_details"   #needs to be parameterised based on ADLS location in actual
mode = "upsert"
key1= "der_type"
key2="der_status"
write_upsert_gold_data(df_der_gold, table_name, mode, key1, key2)

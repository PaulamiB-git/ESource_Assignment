# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime

# COMMAND ----------

#get the list of utility companies active
df = spark.read.format("delta").table("utility_list")
df=df.filter(df.active_status==1).select("utility_name")
utility_list=[row["utility_name"] for row in df.collect()]
print(utility_list)

# COMMAND ----------


#This needs to be parameter of the workflow
dbutils.widgets.dropdown(name='utility_name',defaultValue='utility1',choices=utility_list, label='Utility Name')
utility_name = dbutils.widgets.get("utility_name")


# COMMAND ----------

#run the specific utility flow
# Get the current month
current_month = datetime.now().month
current_year = datetime.now().year
processed_timestamp = datetime.now()

params = {
    "year": current_year,
    "month": current_month,
    "processed_timestamp" : processed_timestamp
}

#print(processed_timestamp)

# COMMAND ----------

notebook_path_network = f"{utility_name}/create_bronze_network_table"
print(notebook_path, current_month, current_year, processed_timestamp)
dbutils.notebook.run(notebook_path_network, 60,params)

# COMMAND ----------

notebook_path_installed = f"{utility_name}/create_bronze_installed_table" 
print(notebook_path, current_month, current_year, processed_timestamp)
dbutils.notebook.run(notebook_path_installed, 60,params)

# COMMAND ----------

notebook_path_planned = f"{utility_name}/create_bronze_planned_table"
print(notebook_path, current_month, current_year, processed_timestamp)
dbutils.notebook.run(notebook_path_planned, 60,params)
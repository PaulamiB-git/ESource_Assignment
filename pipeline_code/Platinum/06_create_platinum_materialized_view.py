# Databricks notebook source
# MAGIC %md
# MAGIC # Platinum notebook - common for All Utlities

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
# Get the current month
current_month = datetime.now().month
current_year = datetime.now().year
processed_timestamp = datetime.now()

params = {
    "year": current_year,
    "month": current_month,
    "processed_timestamp" : processed_timestamp
}


# COMMAND ----------

table_name1 = "workspace.gold.gold_network_details"
df_network_gold = read_data(table_name1)
table_name2 = "workspace.gold.gold_der_details"
df_der_gold = read_data(table_name2)

# COMMAND ----------

# MAGIC %md
# MAGIC Not sure how these two needs to be joined - in ideal scenario, we would join them and select only a few columns and rows based on businees requirement , set row/column level security for it. If tables rae relatively small, we can also create views instead of materialized views

# COMMAND ----------

df_network_gold.write.format("delta").mode("overwrite").saveAsTable("gold_network_materialized_view")
df_der_gold.write.format("delta").mode("overwrite").saveAsTable("gold_der_materialized_view")
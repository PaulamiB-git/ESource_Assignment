# Databricks notebook source
# MAGIC %md
# MAGIC # This is the helper notebook having all common functions/utlities

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable 
from pyspark.sql.functions import col

# COMMAND ----------

def write_data(df, table_name, mode, partition_cols, year_of_data, month_of_data):
        if not spark.catalog.tableExists(table_name):
        # If the table does not exist, create it with partitioning
                df.write.format("delta").mode(mode) \
                        .partitionBy(partition_cols) \
                        .saveAsTable(table_name)
        else:
                df.write.format("delta").mode(mode)\
                        .option("replaceWhere", f"year_of_data='{year_of_data}' and month_of_data='{month_of_data}'")\
                        .saveAsTable(table_name)

# COMMAND ----------

def read_data(table_name,year_of_data=None, month_of_data=None):
    if year_of_data==None and month_of_data==None :
         df=spark.read.table(table_name)
    else:
        df=spark.read.table(table_name).filter(f"year_of_data='{year_of_data}' and month_of_data='{month_of_data}'")
    return df


# COMMAND ----------

def write_upsert_data(source_df, table_name, mode, key):
        if not spark.catalog.tableExists(table_name):
        # If the table does not exist, create it with partitioning
                source_df.write.format("delta").mode("overwrite") \
                        .saveAsTable(table_name)
        else:
                # Load the Delta table using the table name
                delta_table = DeltaTable.forName(spark, table_name)

                # Get the list of columns dynamically
                columns = source_df.columns 

                # Generate the update mappings for all columns dynamically
                update_set = {col_name: col(f"s.{col_name}") for col_name in columns}
                
                # Perform the merge with dynamic updates for all columns
                delta_table.alias("t").merge(source_df.alias("s"),
                        f"t.{key} = s.{key}"
                ).whenMatchedUpdate(
                        set=update_set
                ).whenNotMatchedInsert(
                        values=update_set
                ).execute()
               

# COMMAND ----------

def read_silver_der_data(utility, table_name):
    df = spark.createDataFrame([], gold_der_schema)
    df = read_data(table_name) 
    match utility:
        case 'utility1':
           aggregated_df = df.groupBy("der_type","project_status") \
                  .agg(
                      sum("der_count").alias("total_count"),
                      avg("name_plate_rating").alias("avg_nameplate_rating"),
                      sum("total_charge_cesir").alias("total_charge_cesir"),
                      sum("total_charges_construction").alias("total_charges_construction")
                  )
            aggregated_df = aggregated_df.withColumn("total_charges", sum(col("total_charge_cesir"), col("total_charges_construction")).alias("total_charges"))
                                         .drop("total_charge_cesir","total_charges_construction")  
                                         .withColumnRenamed("project_status","der_status")
                                         .withColumn("utility_name",utility)
        case 'utility2':
            aggregated_df = df.groupBy("der_type","der_status") \
                  .agg(
                      count("*").alias("total_count"),
                      avg("der_nameplate_rating").alias("avg_nameplate_rating"),
                      sum("interconnection_cost").alias("total_charges"),
                      sum("total_charges_construction").alias("total_charges_construction")
                  )
            aggregated_df = aggregated_df.withColumn("utility_name",utility)
    return aggregated_df


# COMMAND ----------

def write_upsert_gold_data(source_df, table_name, mode, key1, key2):
        if not spark.catalog.tableExists(table_name):
        # If the table does not exist, create it with partitioning
                source_df.write.format("delta").mode("overwrite") \
                        .saveAsTable(table_name)
        else:
                # Load the Delta table using the table name
                delta_table = DeltaTable.forName(spark, table_name)

                # Get the list of columns dynamically
                columns = source_df.columns 

                # Generate the update mappings for all columns dynamically
                update_set = {col_name: col(f"s.{col_name}") for col_name in columns}
                
                # Perform the merge with dynamic updates for all columns
                delta_table.alias("t").merge(source_df.alias("s"),
                        f"t.{key1} = s.{key1} AND t.{key2} = s.{key2}"
                ).whenMatchedUpdate(
                        set=update_set
                ).whenNotMatchedInsert(
                        values=update_set
                ).execute()
               
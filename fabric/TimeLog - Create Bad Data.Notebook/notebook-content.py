# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a4464320-9e8b-4f8b-948f-c547245d229f",
# META       "default_lakehouse_name": "ERP_RAW",
# META       "default_lakehouse_workspace_id": "<Workspace ID>"
# META     }
# META   }
# META }

# CELL ********************

import pyspark.sql.functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_df = spark.read.parquet("abfss://<Workspace ID>@onelake.dfs.fabric.microsoft.com/a4464320-9e8b-4f8b-948f-c547245d229f/Files/dbo_TimeLog")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(raw_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Duplicate Record

# CELL ********************

raw_df = raw_df.unionByName(raw_df.filter("TimeLogID = 15").withColumn("TimeLogID", F.lit(19)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(raw_df.orderBy("TimeLogID"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # HoursWorked Outside Range

# CELL ********************

raw_df = raw_df.withColumn("HoursWorked", F.when(F.col("TimeLogID") == 10, F.lit(-1)).when(F.col("TimeLogID") == 8, F.lit(8.5)).otherwise(F.col("HoursWorked")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(raw_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Additional Column

# CELL ********************

raw_df = raw_df.withColumn("InvalidColumn", F.lit("Invalid"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(raw_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Write Back

# CELL ********************

raw_df.write.mode("overwrite").parquet("abfss://<Workspace ID>@onelake.dfs.fabric.microsoft.com/a4464320-9e8b-4f8b-948f-c547245d229f/Files/dbo_TimeLog_BadData")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

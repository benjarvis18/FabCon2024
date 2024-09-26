# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4e0ccd94-8fd0-47b9-a6b5-a0313dcea027",
# META       "default_lakehouse_name": "ProServDataProduct",
# META       "default_lakehouse_workspace_id": "<Workspace ID>"
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS `ProServDataProduct`.`dim_project`
# MAGIC (
# MAGIC     project_key int,
# MAGIC     client_name string,
# MAGIC     project_id int,
# MAGIC     project_name string,
# MAGIC     start_date date,
# MAGIC     end_date date,
# MAGIC     status string,
# MAGIC     budget decimal(19,4),
# MAGIC     inserted_datetime timestamp,
# MAGIC     updated_datetime timestamp
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

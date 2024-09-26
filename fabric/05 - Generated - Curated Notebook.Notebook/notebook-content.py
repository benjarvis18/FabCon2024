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
# MAGIC MERGE INTO  `ProServDataProduct`.`dim_project` AS W
# MAGIC USING       `ProServDataProduct`.`transform_dim_project` AS T
# MAGIC ON           W.project_id = T.project_id            
# MAGIC WHEN MATCHED 
# MAGIC     AND W.client_name != T.client_name
# MAGIC     OR W.project_id != T.project_id
# MAGIC     OR W.project_name != T.project_name
# MAGIC     OR W.start_date != T.start_date
# MAGIC     OR W.end_date != T.end_date
# MAGIC     OR W.status != T.status
# MAGIC     OR W.budget != T.budget
# MAGIC THEN
# MAGIC     UPDATE SET
# MAGIC         client_name = T.client_name,
# MAGIC         project_id = T.project_id,
# MAGIC         project_name = T.project_name,
# MAGIC         start_date = T.start_date,
# MAGIC         end_date = T.end_date,
# MAGIC         status = T.status,
# MAGIC         budget = T.budget,
# MAGIC         updated_datetime = current_timestamp()
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN
# MAGIC     INSERT
# MAGIC     (
# MAGIC         client_name,
# MAGIC         project_id,
# MAGIC         project_name,
# MAGIC         start_date,
# MAGIC         end_date,
# MAGIC         status,
# MAGIC         budget,
# MAGIC         inserted_datetime,
# MAGIC         updated_datetime
# MAGIC     )
# MAGIC     VALUES
# MAGIC     (
# MAGIC         client_name,
# MAGIC         project_id,
# MAGIC         project_name,
# MAGIC         start_date,
# MAGIC         end_date,
# MAGIC         status,
# MAGIC         budget,
# MAGIC         current_timestamp(),
# MAGIC         current_timestamp()
# MAGIC     )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

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
# MAGIC CREATE OR REPLACE TABLE `ProServDataProduct`.`transform_dim_project`
# MAGIC AS
# MAGIC     SELECT  C.ClientName AS client_name,
# MAGIC             P.ProjectID AS project_id,
# MAGIC             P.ProjectName AS project_name,
# MAGIC             P.StartDate AS start_date,
# MAGIC             P.EndDate AS end_date,
# MAGIC             P.Status AS status,
# MAGIC             P.Budget AS budget
# MAGIC     FROM    ERP_BASE.dbo_Project P
# MAGIC             INNER JOIN ERP_BASE.dbo_Client C ON C.ClientID = P.ClientID

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

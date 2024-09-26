# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "<Lakehouse ID>",
# META       "default_lakehouse_name": "Metadata",
# META       "default_lakehouse_workspace_id": "<Workspace ID>"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Read Data Contract JSON Files

# CELL ********************

import pyspark.sql.functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data_contract_df = spark.read.option("multiline", "true").json("abfss://<Workspace ID>@onelake.dfs.fabric.microsoft.com/<Lakehouse ID>/Files/ingestion/*.json")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ingestion_metadata_df = data_contract_df \
    .select(
        F.col("code"),
        F.col("name"),
        F.col("data_source.source_query").alias("source_query")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(ingestion_metadata_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ingestion_metadata_df.write.saveAsTable("ingestion_entity")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

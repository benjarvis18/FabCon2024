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

# # Import Core Code

# CELL ********************

%run "./00 - Core Code"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Get Entities to Process

# CELL ********************

from pathlib import Path

curated_entities = [Path(c.name).stem for c in mssparkutils.fs.ls(f"abfss://<Workspace ID>@onelake.dfs.fabric.microsoft.com/<Lakehouse ID>/Files/curation/")]
contracts_to_process = [DataContractService.get_curated_data_contract(e) for e in curated_entities]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Helper Functions

# CELL ********************

import sempy.fabric as fabric
import base64
from sempy.fabric.exceptions import FabricHTTPException, WorkspaceNotFoundException

fabric_client = fabric.FabricRestClient()

def create_notebook(notebook_name, notebook_content):
    payload = {
        "displayName": notebook_name,
        "type": "Notebook",
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "artifact.content.ipynb",
                    "payload": base64.b64encode(notebook_content.encode("unicode_escape")).decode("utf-8"),
                    "payloadType": "InlineBase64"
                }
            ]
        }
    }

    print(payload)

    workspaceId = fabric.get_workspace_id()
    response = fabric_client.post(f"/v1/workspaces/{workspaceId}/items",json=payload)

    if response.status_code != 202:
        raise FabricHTTPException(response)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_autogen_notebook(notebook_name, sql):
    notebook_content = """{"nbformat":4,"nbformat_minor":5,"cells":[{"cell_type":"code","source":["%%sql\nSQL_STATEMENT\n"],"execution_count":null,"outputs":[],"metadata":{}}],"metadata":{"language_info":{"name":"python"}}}"""
    notebook_content = notebook_content.replace("SQL_STATEMENT", sql)

    create_notebook(notebook_name, notebook_content)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebook_name_prefix = "run7"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Generate Curated Table Definition

# CELL ********************

for data_contract in contracts_to_process:
    columns = [a.name + " " + a.data_type.sql_data_type for a in data_contract.attributes]
    column_list = ",\n\t".join(columns)

    notebook_definition = f"""
CREATE TABLE IF NOT EXISTS `ProServDataProduct`.`{data_contract.code}`
(
    {column_list},
    inserted_datetime timestamp,
    updated_datetime timestamp
)
    """

    print(notebook_definition)

    create_autogen_notebook(f"{notebook_name_prefix}_curated_table_{data_contract.code}", notebook_definition)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Generate Transform Notebook

# CELL ********************

for data_contract in contracts_to_process:
    columns = [a.data_type.default_value + " AS " + a.name for a in data_contract.attributes if not a.is_surrogate_key]
    column_list = ",\n\t\t".join(columns)

    notebook_definition = f"""    
CREATE OR REPLACE TABLE `ProServDataProduct`.`transform_{data_contract.code}`
AS
    SELECT  {column_list}
    """

    print(notebook_definition)

    create_autogen_notebook(f"{notebook_name_prefix}_transform_{data_contract.code}", notebook_definition)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Generate Curated Notebook

# CELL ********************

for data_contract in contracts_to_process:
    natural_keys = [f"W.{a.name} = T.{a.name}" for a in data_contract.attributes if a.is_natural_key]
    type1_scd_columns = [f"W.{a.name} != T.{a.name}" for a in data_contract.attributes if a.is_type1_scd]
    type1_scd_update_columns = [f"{a.name} = T.{a.name}"  for a in data_contract.attributes if a.is_type1_scd]
    insert_columns = [a.name for a in data_contract.attributes if not a.is_surrogate_key]
    surrogate_key = next(iter([a.name for a in data_contract.attributes if a.is_surrogate_key]))

    natural_key_equality = "\n\t AND ".join(natural_keys)
    type1_scd_equality = "\n\tOR ".join(type1_scd_columns)
    type1_scd_update = ",\n\t".join(type1_scd_update_columns)
    insert_column_list = ",\n\t".join(insert_columns)
    insert_values = ",\n\t".join(insert_columns)
    
    columns = [a.data_type.default_value + " AS " + a.name for a in data_contract.attributes]
    column_list = ",\n\t\t".join(columns)

    notebook_definition = f"""    
MERGE INTO  `ProServDataProduct`.`{data_contract.code}` AS W
USING       `ProServDataProduct`.`transform_{data_contract.code}` AS T
ON           {natural_key_equality}            
WHEN MATCHED 
    AND {type1_scd_equality}
THEN
    UPDATE SET
        {type1_scd_update},
        updated_datetime = current_timestamp()
WHEN NOT MATCHED 
THEN
    INSERT
    (
        {insert_column_list},
        inserted_datetime,
        updated_datetime
    )
    VALUES
    (
        {insert_values},
        current_timestamp(),
        current_timestamp()
    )
"""

    print(notebook_definition)

    create_autogen_notebook(f"{notebook_name_prefix}_curated_{data_contract.code}", notebook_definition)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

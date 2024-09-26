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

%run "./00 - Core Code"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils
DAG = {
    "activities": [
        {   
            "name": "dbo_Client",
            "path": "04 - DQ and Cleansing",
            "args": {
                "EntityCode": "dbo_Client",
                "CurationStage": "BASE"                    
            }
        },
        {   
            "name": "dbo_Employee",
            "path": "04 - DQ and Cleansing",
            "args": {
                "EntityCode": "dbo_Employee",
                "CurationStage": "BASE"                    
            }
        },
        {   
            "name": "dbo_Project",
            "path": "04 - DQ and Cleansing",
            "args": {
                "EntityCode": "dbo_Project",
                "CurationStage": "BASE"                    
            }
        },
        {   
            "name": "dbo_ProjectAssignment",
            "path": "04 - DQ and Cleansing",
            "args": {
                "EntityCode": "dbo_ProjectAssignment",
                "CurationStage": "BASE"                    
            }
        },
        {   
            "name": "dbo_TimeLog",
            "path": "04 - DQ and Cleansing",
            "args": {
                "EntityCode": "dbo_TimeLog",
                "CurationStage": "BASE"                    
            }
        }
    ]
}

mssparkutils.notebook.runMultiple(DAG)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

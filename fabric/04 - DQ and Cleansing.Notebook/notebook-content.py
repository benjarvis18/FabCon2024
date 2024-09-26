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
# META       "default_lakehouse_workspace_id": "<Workspace ID>",
# META       "known_lakehouses": [
# META         {
# META           "id": "a4464320-9e8b-4f8b-948f-c547245d229f"
# META         },
# META         {
# META           "id": "<Lakehouse ID>"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Parameters

# PARAMETERS CELL ********************

EntityCode = "dbo_TimeLog"
CurationStage = "BASE"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Import Core Code

# CELL ********************

import pyspark.sql.functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run "./00 - Core Code"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Load Metadata and RAW Data

# CELL ********************

data_contract = DataContractService.get_data_contract(EntityCode)
validation_errors = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_df = spark.read.parquet(f"abfss://<Workspace ID>@onelake.dfs.fabric.microsoft.com/a4464320-9e8b-4f8b-948f-c547245d229f/Files/{EntityCode}")

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

# # Validate Schema

# CELL ********************

def get_data_type(df, column_name):
    return [dtype for name, dtype in df.dtypes if name == column_name][0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for attribute in data_contract.attributes:
    # Validate all expected columns exist in the data frame
    if attribute.name not in raw_df.columns:
        validation_errors.append(
            ValidationError(
                table_name=EntityCode, 
                column_name=attribute.name, 
                curation_stage=CurationStage, 
                error_message=f"Column {attribute.name} does not exist.", 
                error_level=ErrorLevelEnum.error
            )
        )    

        continue

    # Validate data types
    data_type = get_data_type(raw_df, attribute.name)

    if data_type != attribute.data_type.sql_data_type:
        validation_errors.append(
            ValidationError(
                table_name=EntityCode, 
                column_name=attribute.name, 
                curation_stage=CurationStage, 
                error_message=f"Column {attribute.name} data type does not match the data contract. Expected: {attribute.data_type.sql_data_type}, Actual: {data_type}", 
                error_level=ErrorLevelEnum.error
            )
        )  

    # Check nullability
    if not attribute.data_type.is_nullable:
        null_count = raw_df.filter(F.col(attribute.name).isNull()).count()

        if null_count > 0:
            validation_errors.append(
            ValidationError(
                table_name=EntityCode, 
                column_name=attribute.name, 
                curation_stage=CurationStage, 
                error_message=f"Column {attribute.name} is not nullable but {null_count} nulls were found.", 
                error_level=ErrorLevelEnum.error
            )
        )  

# Identify columns that exists in the data frame but not in the contract
expected_attributes = [a.name for a in data_contract.attributes]

for c in raw_df.columns:
    if c not in expected_attributes:
        validation_errors.append(
            ValidationError(
                table_name=EntityCode, 
                column_name=c, 
                curation_stage=CurationStage, 
                error_message=f"Column {c} is included in the source file but does not exist in the data contract. Column will be excluded.", 
                error_level=ErrorLevelEnum.warning
            )
        )

raw_df = raw_df.select(expected_attributes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Run DQ Checks

# CELL ********************

# Validate Natural Key
natural_key_columns = [a.name for a in data_contract.attributes if a.is_natural_key]

if len(natural_key_columns) > 0:
    duplicate_count = raw_df.groupBy(natural_key_columns).count().filter("count > 1").count()

    if duplicate_count > 0:
        natural_key_column_list = ", ".join(natural_key_columns)

        validation_errors.append(
            ValidationError(
                table_name=EntityCode, 
                curation_stage=CurationStage, 
                error_message=f"{duplicate_count} duplicate records found when checking natural key columns ({natural_key_column_list})", 
                error_level=ErrorLevelEnum.error
            )
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def execute_assertion(df, entity_code, curation_stage, attribute_name, assertion):
    match assertion.type:
        case AssertionTypeEnum.range:            
            filter_statement = f"{attribute_name} <= {assertion.min} OR {attribute_name} > {assertion.max}"
            print(filter_statement)

            invalid_count = df.filter(filter_statement).count()

            if invalid_count > 0:
                return ValidationError(
                    table_name=entity_code, 
                    curation_stage=curation_stage, 
                    error_message=f"{invalid_count} records outside the defined range for {attribute_name} (greater than {assertion.min} and less than or equal to {assertion.max})", 
                    error_level=assertion.error_level
                )
        case _:
            raise "Unexpected assertion type."

    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Execute Assertions
for attribute in data_contract.attributes:
    if len(attribute.assertions) > 0:
        for assertion in attribute.assertions:
            result = execute_assertion(raw_df, EntityCode, CurationStage, attribute.name, assertion)

            if result:
                validation_errors.append(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Write Validation Errors to Table and Fail On Error

# CELL ********************

import pandas as pd

if len(validation_errors) > 0:
    validation_errors_df = spark.createDataFrame(pd.DataFrame([v.dict() for v in validation_errors]))
    display(validation_errors_df)

    validation_errors_df \
        .withColumn("log_datetime", F.current_timestamp()) \
        .write.mode("append") \
        .saveAsTable("Audit.validation_log")

    error_count = validation_errors_df.filter("error_level = 'error'").count()

    if error_count > 0:
        raise Exception(f"{error_count} validation error(s) found.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Write to Table

# CELL ********************

base_table_name = f"ERP_BASE.{data_contract.code}"
raw_df.write.mode("overwrite").saveAsTable(base_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Install Dependencies

# CELL ********************

!pip install pydantic

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

!pip install pyodbc

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Import Dependencies

# CELL ********************

from typing import List, Optional
from pydantic import BaseModel
from enum import Enum
import pyodbc
import json
import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Define Pydantic Models for Data Contracts

# CELL ********************

class ErrorLevelEnum(str, Enum):
    info="information",
    warning="warning",
    error="error"

class ValidationError(BaseModel):
    table_name: str
    column_name: Optional[str]
    curation_stage: str
    error_message: str
    error_level: ErrorLevelEnum

class DataTypeEnum(str, Enum):
  string="string",
  integer="integer",
  decimal="decimal",
  date="date",
  boolean="boolean"

class AssertionTypeEnum(str, Enum):
    range="range"

class AttributeDataType(BaseModel):
    type: DataTypeEnum
    max_length: Optional[int]
    precision: Optional[int]
    scale: Optional[int]
    is_nullable: bool

    @property
    def sql_data_type(self):
        match self.type:
            case DataTypeEnum.integer:
                return "int"
            case DataTypeEnum.decimal:
                return f"decimal({self.precision},{self.scale})"
            case _:
                return self.type

    @property
    def default_value(self):
        match self.type:
            case DataTypeEnum.string:
                return "'Unknown'"
            case DataTypeEnum.integer:
                return "-1"
            case DataTypeEnum.decimal:
                return "-1"
            case DataTypeEnum.date:
                return "'1900-01-01'"
            case DataTypeEnum.boolean:
                return "0"

class Assertion(BaseModel):
    type: AssertionTypeEnum
    min: int
    max: int
    error_level: ErrorLevelEnum

class Attribute(BaseModel):
    name: str
    data_type: AttributeDataType   
    assertions: List[Assertion] = []     
    is_natural_key: bool = False
    is_surrogate_key: bool = False
    is_type1_scd: bool = False
    is_type2_scd: bool = False

class DataSource(BaseModel):
    source_query: str

class DataContract(BaseModel):
    code: str
    name: str
    description: str
    data_source: Optional[DataSource]
    attributes: List[Attribute]
    assertions: List[Assertion] = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.mount("abfss://<Workspace ID>@onelake.dfs.fabric.microsoft.com/<Lakehouse ID>", "/lakehouse/Metadata")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class DataContractService:
    @staticmethod
    def get_data_contract(entity_code):
        contract_path = notebookutils.fs.getMountPath(f"/lakehouse/Metadata/Files/ingestion/{entity_code}.json")

        with open(contract_path, 'r') as contract_file:
            content = json.loads(contract_file.read())
            return DataContract(**content)

    @staticmethod
    def get_curated_data_contract(entity_code):
        contract_path = notebookutils.fs.getMountPath(f"/lakehouse/Metadata/Files/curation/{entity_code}.json")

        with open(contract_path, 'r') as contract_file:
            content = json.loads(contract_file.read())
            return DataContract(**content)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

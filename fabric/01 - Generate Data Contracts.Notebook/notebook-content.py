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

%run "./00 - Connections"

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

# # Get Metadata from SQL

# CELL ********************

def get_sql_connection():
    conn = pyodbc.connect(SQL_CONNECTION_STRING)
    return conn

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_tables():
    sql = """    
    SELECT  TABLE_SCHEMA AS 'table_schema',
            TABLE_NAME AS 'table_name',
            CONCAT(TABLE_SCHEMA, '_', TABLE_NAME) AS 'code',
            CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) AS 'name',
            CONCAT('The ', TABLE_SCHEMA, '.', TABLE_NAME, ' table.') AS 'description',
            CONCAT('SELECT * FROM ', QUOTENAME(TABLE_SCHEMA), '.', QUOTENAME(TABLE_NAME)) AS 'source_query'
    FROM    INFORMATION_SCHEMA.TABLES
    WHERE   TABLE_TYPE = 'BASE TABLE'
    """

    rows = []
    with get_sql_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(sql)

        for row in cursor.fetchall():
            rows.append(row)

    return rows

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_columns(table_schema, table_name):
    sql = """
    SELECT  COLUMN_NAME AS 'name',
            CASE 
                WHEN DATA_TYPE = 'int' THEN 'integer'
                WHEN DATA_TYPE LIKE '%varchar' THEN 'string'
                WHEN DATA_TYPE = 'decimal' THEN 'decimal'
                WHEN DATA_TYPE = 'date' THEN 'date'
                WHEN DATA_TYPE = 'bit' THEN 'boolean'
                ELSE DATA_TYPE
            END AS 'data_type_type',
            CHARACTER_MAXIMUM_LENGTH AS 'data_type_max_length',
            NUMERIC_PRECISION AS 'data_type_precision',
            NUMERIC_SCALE AS 'data_type_scale',
            CASE IS_NULLABLE
                WHEN 'YES' THEN 'true'
                WHEN 'NO' THEN 'false'
            END AS 'data_type_is_nullable'
    FROM    INFORMATION_SCHEMA.COLUMNS
    WHERE   TABLE_SCHEMA = ?
            AND TABLE_NAME = ?
    """

    rows = []
    with get_sql_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(sql, table_schema, table_name)

        for row in cursor.fetchall():
            rows.append(row)

    return rows

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def convert_to_attributes(columns):
    attributes = []

    for column in columns:
        attribute = Attribute(
            name=column.name, 
            data_type=AttributeDataType(
                type=column.data_type_type,
                max_length=column.data_type_max_length,
                precision=column.data_type_precision,
                scale=column.data_type_scale,
                is_nullable=column.data_type_is_nullable
            )
        )

        attributes.append(attribute)

    return attributes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Generate Data Contracts

# CELL ********************

tables = get_tables()
contracts = []

for table in tables:
    columns = get_columns(table.table_schema, table.table_name)

    contract = DataContract(
        code=table.code, 
        name=table.name, 
        description=table.description,
        data_source=DataSource(source_query=table.source_query),
        attributes=convert_to_attributes(columns)
    )

    print(f"Processing {table.code}")

    contracts.append(contract)    

    output_path = f"/lakehouse/default/Files/ingestion/{table.code}.json"

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        f.write(json.dumps(contract.dict(), indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

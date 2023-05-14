import os
import sys
import mysql.connector
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import cx_Oracle
import logging
import traceback

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s"
)
logger = logging.getLogger(__name__)

def log_error(custom_message, exception=None):
    if exception:
        exception_message = f"{type(exception).__name__}: {str(exception)}"
        traceback_text = traceback.format_exc()
        formatted_traceback = "\n    ".join(traceback_text.splitlines())
        log_message = f"{custom_message}\n\nException: {exception_message}\n\nTraceback:\n    {formatted_traceback}"
    else:
        log_message = custom_message

    logger.error(log_message)



# Read database configurations from CSV files
try:
    source_config = pd.read_csv("config/source_database_config.csv").iloc[0]
    target_config = pd.read_csv("config/target_database_config.csv").iloc[0]
except FileNotFoundError as e:
    log_error(f"Configuration file not found: {e.filename}")
    sys.exit(1)

# Source database connection
try:
    if source_config["type"] == "mysql":
        source_connection = mysql.connector.connect(
            host=source_config["host"],
            user=source_config["user"],
            password=source_config["password"],
            database=source_config["database"],
        )
        source_cursor = source_connection.cursor()
    elif source_config["type"] == "oracle":
        dsn_str = cx_Oracle.makedsn(source_config["host"], source_config["port"], service_name=source_config["service_name"])
        source_connection = cx_Oracle.connect(user=source_config["user"], password=source_config["password"], dsn=dsn_str)
        source_cursor = source_connection.cursor()
    elif source_config["type"] == "postgresql":
        # Add PostgreSQL connection code here
        pass
except (mysql.connector.Error, cx_Oracle.DatabaseError) as e:
    log_error(f"Error connecting to source database: {e}")
    sys.exit(1)

# Target database connection
try:
    if target_config["type"] == "bigquery":
        credentials = service_account.Credentials.from_service_account_file(target_config["credentials_path"])
        target_client = bigquery.Client(credentials=credentials, project=target_config["project_id"])
except (FileNotFoundError, ValueError) as e:
    log_error(f"Error loading target database credentials: {e}")
    sys.exit(1)
except Exception as e:
    log_error(f"Error connecting to target database: {e}")
    sys.exit(1)

# Read input CSV file
try:
    input_csv = "config/tables_to_compare.csv"
    table_pairs = pd.read_csv(input_csv)
except FileNotFoundError as e:
    log_error(f"Input file not found: {e.filename}")
    sys.exit(1)

# Function to get source table column data
def get_source_column_data(table_name):
    source_cursor.execute(f"DESCRIBE {table_name}")
    columns = source_cursor.fetchall()
    return columns

# Function to get target table column data
def get_target_column_data(table_name):
    try:
        dataset_id, table_id = table_name.split(".")
        table_ref = target_client.dataset(dataset_id).table(table_id)
        table = target_client.get_table(table_ref)
        columns = [(schema_field.name, schema_field.field_type) for schema_field in table.schema]
        return columns
    except Exception as e:
        log_error(f"Error fetching target column data for table '{table_name}': {e}")
        raise


# Function to validate source and target tables based on input configurations
def validate_tables(row):
    # Extract table pair and validation configurations from the row
    source_table_name = row["source_table_name"]
    target_table_name = row["target_table_name"]
    column_name_check = row["column_name_check"]
    column_name_check_case_sensitive = row["column_name_check_case_sensitive"]
    row_count_check = row["row_count_check"]
    data_type_check = row["data_type_check"]
    null_count_check = row["null_count_check"]

    # Get column data for source and target tables
    source_columns = get_source_column_data(source_table_name)
    target_columns = get_target_column_data(target_table_name)

    # Initialize validation results
    results = []

    # Perform column name check
    if column_name_check:
        for source_col, target_col in zip(source_columns, target_columns):
            source_col_name = source_col[0]
            target_col_name = target_col[0]

            if column_name_check_case_sensitive:
                if source_col_name != target_col_name:
                    results.append({
                        "validation": "column_name_check",
                        "source_column": source_col_name,
                        "target_column": target_col_name,
                        "status": "Mismatch",
                    })
            else:
                if source_col_name.lower() != target_col_name.lower():
                    results.append({
                        "validation": "column_name_check",
                        "source_column": source_col_name,
                        "target_column": target_col_name,
                        "status": "Mismatch",
                    })

    # Perform data type check
    if data_type_check:
        for source_col, target_col in zip(source_columns, target_columns):
            source_col_type = source_col[1]
            target_col_type = target_col[1]

            if source_col_type != target_col_type:
                results.append({
                    "validation": "data_type_check",
                    "source_column": source_col[0],
                    "source_data_type": source_col_type,
                    "target_column": target_col[0],
                    "target_data_type": target_col_type,
                    "status": "Mismatch",
                })

    # Perform row count check and null count check
    if row_count_check or null_count_check:
        source_cursor.execute(f"SELECT COUNT(*) FROM {source_table_name}")
        source_row_count = source_cursor.fetchone()[0]

        query_job = target_client.query(f"SELECT COUNT(*) FROM {target_table_name}")
        target_row_count = query_job.result().to_dataframe().iloc[0, 0]

        if row_count_check:
            results.append({
                "validation": "row_count_check",
                "source_row_count": source_row_count,
                "target_row_count": target_row_count,
                "status": "Match" if source_row_count == target_row_count else "Mismatch",
            })

    if null_count_check:
        for source_col, target_col in zip(source_columns, target_columns):
            source_col_name = source_col[0]
            target_col_name = target_col[0]

            source_cursor.execute(f"SELECT COUNT(*) FROM {source_table_name} WHERE {source_col_name} IS NULL")
            source_null_count = source_cursor.fetchone()[0]

            query_job = target_client.query(f"SELECT COUNT(*) FROM {target_table_name} WHERE {target_col_name} IS NULL")
            target_null_count = query_job.result().to_dataframe().iloc[0, 0]

            results.append({
                "validation": "null_count_check",
                "column": source_col_name,
                "source_null_count": source_null_count,
                "target_null_count": target_null_count,
                "status": "Match" if source_null_count == target_null_count else "Mismatch",
            })

    # Save validation results to a CSV file
    output_filename = f"validation_results_{source_table_name}_to_{target_table_name}.csv"
    output_df = pd.DataFrame(results)
    output_df.to_csv(output_filename, index=False)

# Run validations
for _, row in table_pairs.iterrows():
    try:
        validate_tables(row)
    except Exception as e:
        log_error(f"Error validating tables: {e}")



import os
import pyodbc
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import logging
from typing import List, Dict, Any
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

load_dotenv()


SQL_SERVER = os.getenv('SQL_SERVER_HOST')
SQL_DATABASE = os.getenv('SQL_DATABASE')
SQL_USERNAME = os.getenv('SQL_USERNAME')
SQL_PASSWORD = os.getenv('SQL_PASSWORD')

# Destination: Google Cloud & BigQuery
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BQ_RAW_DATASET = os.getenv('BQ_RAW_DATASET', 'analytics_raw')

# Script Settings
CHUNKSIZE = 100000
LOAD_TIMESTAMP_COLUMN = 'loaded_at_ts'


TABLES_TO_INGEST: List[Dict[str, Any]] = [
    # {"source_name": "dbo.HoaDon_ChiTietHangHoa", "dest_name": "raw_hoadon_chitiethanghoa", "pk": "id"},
    {"source_name": "dbo.CuaHang", "dest_name": "raw_cuahang", "pk": "id"},
    {"source_name": "dbo.Ca", "dest_name": "raw_ca", "pk": "id"},
    {"source_name": "dbo.HoaDon", "dest_name": "raw_transactions", "pk": "id"},
    {"source_name": "dbo.KhachHang", "dest_name": "raw_customer", "pk": "id"},
    {"source_name": "dbo.HoaDon_GiamGia", "dest_name": "raw_sql_hoadon_giamgia", "pk": "id"}

]

def get_sql_connection_string() -> str:
    """
    Builds a robust connection string that explicitly searches for modern,
    correctly functioning ODBC drivers.
    """
    preferred_drivers = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]
    driver_to_use = next((driver for driver in preferred_drivers if driver in pyodbc.drivers()), None)

    if not driver_to_use:
        available_drivers = pyodbc.drivers()
        raise RuntimeError(
            f"No modern SQL Server ODBC driver found. "
            f"Please install 'ODBC Driver 17 for SQL Server' or a newer version. "
            f"Your system has these drivers available: {available_drivers}"
        )

    logging.info(f"Using modern ODBC driver: '{driver_to_use}'")
    
    conn_str = (
        f"DRIVER={{{driver_to_use}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )
    return conn_str

def merge_staging_to_final_bigquery(client: bigquery.Client, table_config: Dict[str, Any], staging_table_id: str):
    """
    Executes a MERGE statement to create and populate the final destination table
    from the staging table. This is an idempotent way to perform an initial load.
    """
    dest_name = table_config['dest_name']
    pk = table_config['pk']
    final_table_id = f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{dest_name}"

    logging.info(f"Preparing to merge data from {staging_table_id} into {final_table_id}.")
    
    staging_table = client.get_table(staging_table_id)

    final_table = bigquery.Table(final_table_id, schema=staging_table.schema)
    client.create_table(final_table, exists_ok=True)
    

    update_clauses = [f"T.`{field.name}` = S.`{field.name}`" for field in staging_table.schema if field.name != pk]
    insert_columns = [f"`{field.name}`" for field in staging_table.schema]
    source_columns = [f"S.`{field.name}`" for field in staging_table.schema]

    merge_query = f"""
    MERGE `{final_table_id}` T
    USING `{staging_table_id}` S
    ON T.`{pk}` = S.`{pk}`
    WHEN NOT MATCHED THEN
        INSERT ({', '.join(insert_columns)})
        VALUES ({', '.join(source_columns)})
    """
    
    logging.info(f"Executing MERGE query for {dest_name}...")
    merge_job = client.query(merge_query)
    merge_job.result() 
    
    logging.info(f"MERGE complete for {dest_name}. {merge_job.num_dml_affected_rows} rows were affected.")

def process_table(client: bigquery.Client, table_config: Dict[str, Any]):
    """
    Performs the full load for a single table, but only if it doesn't already exist.
    """
    dest_name = table_config['dest_name']
    final_table_id = f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{dest_name}"

    try:
        client.get_table(final_table_id)
        logging.info(f"Table '{final_table_id}' already exists. SKIPPING initial load.")
        return 
    except NotFound:
        logging.info(f"Table '{final_table_id}' not found. Proceeding with initial historical load.")

    logging.info(f"--- Starting full snapshot for table: {dest_name} ---")

    conn_str = get_sql_connection_string()
    sql_query = f"SELECT * FROM {table_config['source_name']}"
    logging.info(f"Executing query: {sql_query}")

    timestamp_str = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    staging_table_id = f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.staging_{dest_name}_{timestamp_str}"
    
    total_rows = 0
    try:
        load_timestamp = datetime.now(timezone.utc)
        
        with pyodbc.connect(conn_str, autocommit=True) as conn:
            chunk_iterator = pd.read_sql(sql_query, conn, chunksize=CHUNKSIZE)
            
            for i, chunk_df in enumerate(chunk_iterator):
                if chunk_df.empty: continue
                
                total_rows += len(chunk_df)
                chunk_df.columns = [col.lower().replace(' ', '_').replace('[', '').replace(']', '') for col in chunk_df.columns]
                
                chunk_df[LOAD_TIMESTAMP_COLUMN] = load_timestamp
                
                logging.info(f"Loading chunk {i+1} with {len(chunk_df)} rows into staging table {staging_table_id}.")
                chunk_df.to_gbq(
                    destination_table=staging_table_id.split('.', 1)[1], # pandas-gbq needs dataset.table
                    project_id=GCP_PROJECT_ID,
                    if_exists='append'
                )
        
        if total_rows > 0:
            merge_staging_to_final_bigquery(client, table_config, staging_table_id)
        else:
            logging.info(f"No rows found in source for table {dest_name}. Skipping.")
            
    finally:
        logging.info(f"Dropping staging table {staging_table_id}.")
        client.delete_table(staging_table_id, not_found_ok=True)
            
    logging.info(f"Successfully processed {total_rows} rows for {dest_name}.")

def main():
    """Main entry point for the script."""
    logging.info("--- Starting ELT Historical Load Pipeline for BigQuery ---")
    bigquery_client = bigquery.Client(project=GCP_PROJECT_ID)
    
    try:
        bigquery_client.get_dataset(BQ_RAW_DATASET)
    except NotFound:
        logging.info(f"Dataset {BQ_RAW_DATASET} not found. Creating it.")
        bigquery_client.create_dataset(BQ_RAW_DATASET)
        
    for table_config in TABLES_TO_INGEST:
        process_table(bigquery_client, table_config)
        
    logging.info("--- All tables processed successfully. ---")

if __name__ == "__main__":
    main()
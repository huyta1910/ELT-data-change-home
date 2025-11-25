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

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BQ_RAW_DATASET = os.getenv('BQ_RAW_DATASET', 'analytics_raw')

CHUNKSIZE = 100000
LOAD_TIMESTAMP_COLUMN = 'loaded_at_ts'

# --- CONFIGURATION ---

TABLES_TO_INGEST: List[Dict[str, Any]] = [
    {"source_name": "dbo.HoaDon_ChiTietHangHoa", "dest_name": "raw_hoadon_chitiethanghoa", "pk": "id", "source_date_col": "created_date"},
    {"source_name": "dbo.CuaHang", "dest_name": "raw_cuahang", "pk": "id", "source_date_col": "created_date"},
    {"source_name": "dbo.Ca", "dest_name": "raw_ca", "pk": "id", "source_date_col": "created_date"},
    {"source_name": "dbo.HoaDon", "dest_name": "raw_transactions", "pk": "id", "source_date_col": "created_date"},
    {"source_name": "dbo.KhachHang", "dest_name": "raw_customer", "pk": "id", "source_date_col": "created_date"},
    {"source_name": "dbo.HoaDon_GiamGia", "dest_name": "raw_sql_hoadon_giamgia", "pk": "id", "source_date_col": "created_date"}
]

def get_sql_connection_string() -> str:
    preferred_drivers = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]
    driver_to_use = next((driver for driver in preferred_drivers if driver in pyodbc.drivers()), None)
    
    if not driver_to_use:
        available = pyodbc.drivers()
        if available: driver_to_use = available[0]
        else: raise RuntimeError("No ODBC drivers found.")

    return (
        f"DRIVER={{{driver_to_use}}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=yes;"
    )

def get_last_pipeline_run_time(client: bigquery.Client, final_table_id: str):
    """
    Checks BigQuery to find the last time this pipeline successfully loaded data
    by looking at the MAX(loaded_at_ts).
    """
    try:
        client.get_table(final_table_id)
        
        # We look for the maximum value of 'loaded_at_ts'
        query = f"SELECT MAX({LOAD_TIMESTAMP_COLUMN}) as last_run FROM `{final_table_id}`"
        job = client.query(query)
        result = list(job.result())
        
        if result and result[0].last_run is not None:
            return result[0].last_run
            
    except NotFound:
        return None 
    except Exception as e:
        logging.warning(f"Could not get last run time: {e}. Defaulting to full load.")
        return None
    return None

def merge_staging_to_final_bigquery(client: bigquery.Client, table_config: Dict[str, Any], staging_table_id: str):
    dest_name = table_config['dest_name']
    pk = table_config['pk']
    final_table_id = f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{dest_name}"
    
    staging_table = client.get_table(staging_table_id)
    final_table = bigquery.Table(final_table_id, schema=staging_table.schema)
    client.create_table(final_table, exists_ok=True)

    # Upsert Logic
    update_clauses = [f"T.`{field.name}` = S.`{field.name}`" for field in staging_table.schema if field.name not in [pk, LOAD_TIMESTAMP_COLUMN]]
    insert_columns = [f"`{field.name}`" for field in staging_table.schema]
    source_columns = [f"S.`{field.name}`" for field in staging_table.schema]

    merge_query = f"""
    MERGE `{final_table_id}` T
    USING `{staging_table_id}` S
    ON T.`{pk}` = S.`{pk}`
    WHEN MATCHED THEN
        UPDATE SET {', '.join(update_clauses)}
    WHEN NOT MATCHED THEN
        INSERT ({', '.join(insert_columns)})
        VALUES ({', '.join(source_columns)})
    """
    
    client.query(merge_query).result()
    logging.info(f"MERGE Success for {dest_name}.")

def process_table(client: bigquery.Client, table_config: Dict[str, Any]):
    dest_name = table_config['dest_name']
    source_name = table_config['source_name']
    source_date_col = table_config.get('source_date_col', 'created_date') # The column in SQL Server
    
    final_table_id = f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{dest_name}"

    # 1. GET LAST RUN TIME FROM BQ (loaded_at_ts)
    last_run_time = get_last_pipeline_run_time(client, final_table_id)
    
    # 2. BUILD SQL QUERY
    if last_run_time is not None:
        # Convert the BQ Timestamp to a string format SQL Server understands
        # Format: YYYY-MM-DD HH:MM:SS.mmm
        val_str = last_run_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        logging.info(f"Incremental Load: Fetching {source_name} where {source_date_col} > Last Run ({val_str})")
        
        # LOGIC: Select from SQL Server where created_date > Last Pipeline Run
        sql_query = f"SELECT * FROM {source_name} WHERE {source_date_col} > '{val_str}'"
    else:
        logging.info(f"Full Load: No previous run found for {dest_name}.")
        sql_query = f"SELECT * FROM {source_name}"

    # 3. LOAD DATA
    conn_str = get_sql_connection_string()
    timestamp_str = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    staging_table_id = f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.staging_{dest_name}_{timestamp_str}"
    
    total_rows = 0
    # This is the timestamp for THIS current run
    current_load_timestamp = datetime.now(timezone.utc)

    try:
        with pyodbc.connect(conn_str, autocommit=True) as conn:
            chunk_iterator = pd.read_sql(sql_query, conn, chunksize=CHUNKSIZE)
            
            for i, chunk_df in enumerate(chunk_iterator):
                if chunk_df.empty: continue
                total_rows += len(chunk_df)
                
                chunk_df.columns = [col.lower().replace(' ', '_').replace('[', '').replace(']', '') for col in chunk_df.columns]
                
                # Apply the CURRENT timestamp to the new data
                chunk_df[LOAD_TIMESTAMP_COLUMN] = current_load_timestamp
                
                logging.info(f"Loading chunk {i+1} ({len(chunk_df)} rows)...")
                chunk_df.to_gbq(
                    destination_table=staging_table_id.split('.', 1)[1],
                    project_id=GCP_PROJECT_ID,
                    if_exists='append'
                )

        if total_rows > 0:
            merge_staging_to_final_bigquery(client, table_config, staging_table_id)
            logging.info(f"Processed {total_rows} rows for {dest_name}.")
        else:
            logging.info(f"No new data found for {dest_name} since {last_run_time}.")

    except Exception as e:
        logging.error(f"Failed to process {dest_name}: {e}")
        raise
    finally:
        client.delete_table(staging_table_id, not_found_ok=True)

def main():
    bigquery_client = bigquery.Client(project=GCP_PROJECT_ID)
    try: bigquery_client.get_dataset(BQ_RAW_DATASET)
    except NotFound: bigquery_client.create_dataset(BQ_RAW_DATASET)
        
    for table_config in TABLES_TO_INGEST:
        process_table(bigquery_client, table_config)

if __name__ == "__main__":
    main()
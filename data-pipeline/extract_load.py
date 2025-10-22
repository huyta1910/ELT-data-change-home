import os
import pyodbc
import pandas as pd
from dotenv import load_dotenv
import clickhouse_connect
import logging
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO)
load_dotenv()


# (Database and ClickHouse configs are the same)
SQL_SERVER = os.getenv('SQL_SERVER_HOST')
SQL_DATABASE = os.getenv('SQL_DATABASE')
SQL_USERNAME = os.getenv('SQL_USERNAME')
SQL_PASSWORD = os.getenv('SQL_PASSWORD')
SQL_ENCRYPT = os.getenv('SQL_ENCRYPT', 'yes').lower()

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', 8123))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB', 'analytics_raw') # Staging database for raw data

CHUNKSIZE = 100000


TABLES_TO_INGEST: List[Dict[str, Any]] = [
    {
        "source_name": "dbo.CuaHang",
        "dest_name": "raw_cuahang",
        "pk": "id", 
        "incremental_key": "modified_at" 
    },
    {
        "source_name": "dbo.Ca",
        "dest_name": "raw_ca",
        "pk": "id",
        "incremental_key": "modified_at"
    },
    {
        "source_name": "dbo.HoaDon",
        "dest_name": "raw_hoadon",
        "pk": "id",
        "incremental_key": "modified_at"
    },
    {
        "source_name": "dbo.HoaDon_GiamGia",
        "dest_name": "raw_hoadon_giamgia",
        "pk": "id",
        "incremental_key": "modified_at"
    },
    {
        "source_name": "dbo.HoaDon_ChiTietHangHoa",
        "dest_name": "raw_hoadon_chitiethanghoa",
        "pk": "id",
        "incremental_key": "modified_at"
    }
]



def pandas_to_clickhouse_schema(df: pd.DataFrame, pk: str) -> str:
    pass # For brevity

def ensure_clickhouse_table_exists(client, table_config: Dict[str, Any], df_schema: pd.DataFrame):
    """Creates a ReplacingMergeTree table based on the configuration."""
    pass 

def get_high_water_mark_clickhouse(client, table_config: Dict[str, Any]) -> str:
    """Queries ClickHouse for the max incremental key value for a specific table."""
    pass 


def process_table(client, table_config: Dict[str, Any]):
    """
    Processes a single table: gets high-water mark, extracts new data,
    and loads it into the corresponding ClickHouse table.
    """
    dest_name = table_config['dest_name']
    logging.info(f"--- Starting processing for table: {dest_name} ---")
    
    high_water_mark = get_high_water_mark_clickhouse(client, table_config)
    
    sql_query = f"SELECT * FROM {table_config['source_name']} WHERE {table_config['incremental_key']} > '{high_water_mark}'"
    logging.info(f"Executing incremental query: {sql_query}")

    driver = next((d for d in pyodbc.drivers() if 'sql server' in d.lower()), None)
    if not driver: raise RuntimeError("No SQL Server ODBC driver found.")
    conn_str = f"DRIVER={{{driver}}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=yes;"
    
    total_rows = 0
    is_first_chunk = True

    try:
        with pyodbc.connect(conn_str, autocommit=True) as conn:
            chunk_iterator = pd.read_sql(sql_query, conn, chunksize=CHUNKSIZE)
            
            for chunk_df in chunk_iterator:
                if chunk_df.empty:
                    continue
                
                total_rows += len(chunk_df)
                chunk_df.columns = [col.lower().replace(' ', '_') for col in chunk_df.columns]
                
                if is_first_chunk:
                    # ensure_clickhouse_table_exists(client, table_config, chunk_df)
                    is_first_chunk = False
                
                logging.info(f"Inserting chunk with {len(chunk_df)} rows into {dest_name}.")
                client.insert_df(f"{CLICKHOUSE_DB}.{dest_name}", chunk_df)
        
        if total_rows == 0:
            logging.info(f"No new data found for table {dest_name}.")
        else:
            logging.info(f"Successfully loaded a total of {total_rows} new/updated rows for {dest_name}.")

    except Exception as e:
        logging.error(f"Failed to process table {dest_name}. Error: {e}")
        raise


def main():
    """Main function to run the incremental pipeline for all configured tables."""
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD
    )
    client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")
    
    for table_config in TABLES_TO_INGEST:
        process_table(client, table_config)
    
    logging.info("--- All tables processed successfully. ---")

if __name__ == "__main__":
    main()
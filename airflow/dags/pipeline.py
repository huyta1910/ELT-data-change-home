# scripts/ingest_incremental.py
import os
import pyodbc
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage, bigquery
import logging
from datetime import datetime, timezone

# --- Setup ---
logging.basicConfig(level=logging.INFO)
load_dotenv()

# --- Configuration ---
# Database and Cloud Config
SQL_SERVER = os.getenv('SQL_SERVER_HOST')
SQL_DATABASE = os.getenv('SQL_DATABASE')
SQL_USERNAME = os.getenv('SQL_USERNAME')
SQL_PASSWORD = os.getenv('SQL_PASSWORD')
SQL_ENCRYPT = os.getenv('SQL_ENCRYPT', 'yes').lower()

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BQ_RAW_DATASET = os.getenv('BQ_RAW_DATASET', 'analytics_raw')
GCS_BUCKET_NAME = f"data-landing-{GCP_PROJECT_ID}" # A unique bucket name

# Incremental Table Configuration
TABLE_NAME = "HoaDon_ChiTietHangHoa"
DEST_TABLE_NAME = "raw_sql_hoadon_chitiethanghoa"
CHUNKSIZE = 100000

# VERY IMPORTANT: Define your keys for incremental logic
INCREMENTAL_KEY = "modified_at"  # The timestamp column in SQL Server
PRIMARY_KEY = "id"               # The unique ID for each row

# --- Helper Functions ---

def get_high_water_mark() -> str:
    """Queries BigQuery to get the latest value from the incremental key column."""
    bigquery_client = bigquery.Client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{DEST_TABLE_NAME}"
    
    try:
        # Check if table exists before querying
        bigquery_client.get_table(table_id)
        query = f"SELECT MAX({INCREMENTAL_KEY}) FROM `{table_id}`"
        logging.info(f"Executing high-water mark query: {query}")
        query_job = bigquery_client.query(query)
        result = query_job.result()
        
        # The result is an iterator of rows
        for row in result:
            high_water_mark = row[0]
            if high_water_mark:
                # Format timestamp to be safe for SQL query
                return high_water_mark.strftime('%Y-%m-%d %H:%M:%S.%f')
        
        # If the table is empty or the max value is NULL
        logging.info("No high-water mark found (table might be empty). Defaulting to a very old date.")
        return "1900-01-01 00:00:00.000"

    except Exception:
        logging.warning(f"Table {table_id} not found. Assuming first run. Defaulting to a very old date.")
        return "1900-01-01 00:00:00.000"


def extract_and_upload_incremental(high_water_mark: str):
    """Extracts new/updated data from SQL Server and uploads it to GCS."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    if not bucket.exists():
        logging.info(f"Bucket {GCS_BUCKET_NAME} not found, creating it...")
        bucket.create(location="US")

    # Dynamic SQL Query using the high-water mark
    sql_query = f"SELECT * FROM dbo.{TABLE_NAME} WHERE {INCREMENTAL_KEY} > '{high_water_mark}'"
    logging.info(f"Executing incremental query: {sql_query}")

    # Your robust connection logic
    driver = next((d for d in pyodbc.drivers() if 'sql server' in d.lower()), None)
    if not driver: raise RuntimeError("No SQL Server ODBC driver found.")
    conn_str = f"DRIVER={{{driver}}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=yes;"
    
    gcs_folder_path = f"{TABLE_NAME}_incremental/{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    total_rows = 0
    
    try:
        with pyodbc.connect(conn_str, autocommit=True) as conn:
            chunk_iterator = pd.read_sql(sql_query, conn, chunksize=CHUNKSIZE)
            
            chunk_num = 0
            for chunk_df in chunk_iterator:
                if chunk_df.empty:
                    logging.info("No new data found in this chunk iteration.")
                    break
                
                chunk_num += 1
                total_rows += len(chunk_df)
                logging.info(f"Processing chunk {chunk_num} with {len(chunk_df)} rows...")
                
                chunk_df.columns = [col.lower().replace(' ', '_') for col in chunk_df.columns]
                local_file_path = f"temp_chunk_{chunk_num}.parquet"
                chunk_df.to_parquet(local_file_path, index=False)
                
                gcs_file_path = f"{gcs_folder_path}/{local_file_path}"
                blob = bucket.blob(gcs_file_path)
                blob.upload_from_filename(local_file_path)
                os.remove(local_file_path)
                
            if total_rows == 0:
                logging.info("No new records to process.")
                return None # Signal that no further action is needed
                
            logging.info(f"All {total_rows} new/updated rows uploaded to GCS folder: {gcs_folder_path}")
            return gcs_folder_path

    except Exception as e:
        logging.error(f"An error occurred during extraction: {e}")
        raise

def merge_gcs_to_bigquery(gcs_folder_path: str):
    """Loads GCS Parquet files into a staging table and merges them into the final table."""
    if not gcs_folder_path:
        logging.info("No GCS folder path provided, skipping MERGE operation.")
        return

    bigquery_client = bigquery.Client()
    staging_table_name = f"staging_{DEST_TABLE_NAME}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    staging_table_id = f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{staging_table_name}"
    final_table_id = f"`{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{DEST_TABLE_NAME}`"
    gcs_uri = f"gs://{GCS_BUCKET_NAME}/{gcs_folder_path}/*"

    # 1. Load data from GCS into a new staging table
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
    )
    logging.info(f"Loading data from GCS into staging table: {staging_table_id}")
    load_job = bigquery_client.load_table_from_uri(gcs_uri, staging_table_id, job_config=job_config)
    load_job.result()

    # 2. Construct and run the MERGE query
    logging.info(f"Merging data from staging table into {final_table_id}")
    # Dynamically create the SET part of the update
    final_table_schema = bigquery_client.get_table(f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{DEST_TABLE_NAME}").schema
    update_clauses = [f"T.`{field.name}` = S.`{field.name}`" for field in final_table_schema if field.name != PRIMARY_KEY]
    
    merge_query = f"""
    MERGE {final_table_id} T
    USING `{staging_table_id}` S
    ON T.{PRIMARY_KEY} = S.{PRIMARY_KEY}
    WHEN MATCHED THEN
        UPDATE SET {', '.join(update_clauses)}
    WHEN NOT MATCHED THEN
        INSERT ROW
    """
    logging.info(f"Executing MERGE query:\n{merge_query}")
    merge_job = bigquery_client.query(merge_query)
    merge_job.result()
    logging.info(f"MERGE complete. {merge_job.num_dml_affected_rows} rows affected.")

    # 3. Clean up staging table and GCS files
    logging.info(f"Dropping staging table: {staging_table_id}")
    bigquery_client.delete_table(staging_table_id, not_found_ok=True)
    
    logging.info("Cleaning up GCS files...")
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=gcs_folder_path)
    for blob in blobs:
        blob.delete()
    logging.info("Cleanup complete.")


def main():
    """Main function to run the incremental pipeline."""
    high_water_mark = get_high_water_mark()
    gcs_folder = extract_and_upload_incremental(high_water_mark)
    if gcs_folder:
        merge_gcs_to_bigquery(gcs_folder)
    else:
        logging.info("Pipeline run finished. No new data was found.")

if __name__ == "__main__":
    main()


from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='incremental_hoadon_chitiethanghoa_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily', # Run once a day
    catchup=False,
    tags=['bigquery', 'incremental'],
) as dag:
    
    run_incremental_ingestion = BashOperator(
        task_id='run_incremental_ingestion',
        # This command runs the Python script from within the Airflow container
        # The script is available because we mounted the `scripts` folder in docker-compose.yml
        bash_command='python /opt/airflow/scripts/ingest_incremental.py'
    )
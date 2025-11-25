import os
import sys
from google.cloud import bigquery

def main():
    keyfile = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', './gcp-credentials.json/caramel-spot-474502-p2-edbabb751408.json')
    project = os.environ.get('GCP_PROJECT_ID', 'caramel-spot-474502-p2')
    dataset_id = 'analytics_raw'
    table_id = 'raw_transactions'

    if not os.path.exists(keyfile):
        print(f'ERROR: service account keyfile not found at {keyfile}', file=sys.stderr)
        sys.exit(2)

    client = bigquery.Client.from_service_account_json(keyfile, project=project)

    ds_ref = bigquery.DatasetReference(project, dataset_id)
    try:
        ds = client.get_dataset(ds_ref)
        print(f'Dataset found: {project}:{dataset_id} (location={ds.location})')
    except Exception as e:
        print(f'Dataset {project}:{dataset_id} not found: {e}', file=sys.stderr)
        sys.exit(3)

    tbl_ref = ds_ref.table(table_id)
    try:
        tbl = client.get_table(tbl_ref)
        print(f'Table found: {project}:{dataset_id}.{table_id} (location={tbl.location})')
    except Exception as e:
        print(f'Table {project}:{dataset_id}.{table_id} not found: {e}', file=sys.stderr)
        sys.exit(4)

    # List tables in dataset
    print('\nTables in dataset:')
    for t in client.list_tables(ds_ref):
        print(' -', t.table_id)

if __name__ == "__main__":
    main()

import os
import csv
from google.cloud import bigquery
from google.cloud import storage

def load_data_into_bigquery(event, context):
    # Get the file name from the event
    file_name = event['name']
    bucket_name = event['bucket']

    # Initialize GCS client and read the CSV file
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    data = blob.download_as_text().splitlines()

    # Parse CSV data
    rows = list(csv.DictReader(data))

    # Initialize BigQuery client and insert rows into table
    bigquery_client = bigquery.Client()
    table_id = os.getenv('BQ_TABLE')  # Use environment variable for table name

    errors = bigquery_client.insert_rows_json(table_id, rows)
    if errors:
        print(f"Errors occurred: {errors}")
    else:
        print(f"Data successfully loaded into {table_id}")

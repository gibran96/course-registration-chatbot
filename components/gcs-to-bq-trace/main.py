import os
import csv
from google.cloud import bigquery
from google.cloud import storage

def load_trace_data_into_bigquery(event, context):
    file_name = event['name']
    bucket_name = event['bucket']

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    pdf_data = blob.download_as_bytes()

    extracted_data = process_pdf(pdf_data)

    bigquery_client = bigquery.Client()
    table_id = os.getenv('BQ_TABLE')  

    errors = bigquery_client.insert_rows_json(table_id, extracted_data)
    if errors:
        print(f"Errors occurred: {errors}")
    else:
        print(f"Data successfully loaded into {table_id}")

def process_pdf(pdf_data):

    ## Put goutham's pdf processing code here

    json_data = []

    ##sample
    # json_data.append({
    #     "name": parts[0].strip(),
    #     "age": int(parts[1].strip())
    # })

    return json_data
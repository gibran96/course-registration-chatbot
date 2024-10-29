from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.data_utils import upload_to_gcs
from scripts.extract_data import process_pdf_files
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import Variable
from google.cloud import storage

import logging
logging.basicConfig(level=logging.INFO)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

def get_crn_list(**context):
    distinct_values = context['ti'].xcom_pull(task_ids='select_distinct_crn')
    distinct_values_list = [row[0] for row in distinct_values]
    logging.info("Distinct values:", distinct_values_list)
    context['ti'].xcom_push(key='crn_list', value=distinct_values_list)
    return list(set(distinct_values_list))


def get_unique_blobs(**context):
    bucket_name = context['dag_run'].conf.get('bucket_name', Variable.get('default_bucket_name'))
    all_gcs_crns = context['ti'].xcom_pull(task_ids='get_crn_list', key='crn_list')

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix='course_review_dataset/')

    unique_blobs = []
    # blob_names = [blob.name.split('/')[-1].replace('.pdf', '') for blob in blobs]
    for blob in blobs:
        blob_name = blob.name.split('/')[-1].replace('.pdf', '')
        if blob_name not in all_gcs_crns:
            unique_blobs.append(blob_name)

    context['ti'].xcom_push(key='unique_blobs', value=unique_blobs)

    return unique_blobs

# Create DAG
with DAG(
    'pdf_processing_pipeline',
    default_args=default_args,
    description='Process PDFs from GCS and extract course review data',
    schedule_interval='0 0 * * *',  # Daily at midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['pdf', 'processing', 'gcs'],
    max_active_runs=1
) as dag:
    
    select_distinct_crn = BigQueryGetDataOperator(
        task_id='select_distinct_crn',
        dataset_id=Variable.get('review_table_name').split('.')[1],
        table_id=Variable.get('review_table_name').split('.')[-1], 
        selected_fields='crn',  
        gcp_conn_id='bigquery_default',
    )

    get_crn_list_task = PythonOperator(
        task_id='get_crn_list',
        python_callable=get_crn_list,
        provide_context=True,
        dag=dag
    )

    unique_blobs = PythonOperator(
        task_id='get_unique_blobs',
        python_callable=get_unique_blobs,
        provide_context=True,
        dag=dag
    )

    # Task to process PDFs and create CSVs
    process_pdfs = PythonOperator(
        task_id='process_pdfs',
        python_callable=process_pdf_files,
        provide_context=True,
        dag=dag
    )

    # Task to upload processed files back to GCS
    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        provide_context=True,
        dag=dag
    )

    load_reviews_to_bigquery_task = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket=Variable.get('default_bucket_name'),
        source_objects=['processed_trace_data/reviews.csv'],
        destination_project_dataset_table=Variable.get('review_table_name'),
        write_disposition='WRITE_APPEND',
        skip_leading_rows=1,
        dag=dag,
    )

    load_courses_to_bigquery_task = GCSToBigQueryOperator(
        task_id='load_courses_to_bigquery',
        bucket=Variable.get('default_bucket_name'),
        source_objects=['processed_trace_data/courses.csv'],
        destination_project_dataset_table=Variable.get('course_table_name'),
        write_disposition='WRITE_APPEND',
        skip_leading_rows=1,
        dag=dag,
    )



    # Set task dependencies
    select_distinct_crn >> get_crn_list_task >> unique_blobs >> process_pdfs >> upload_to_gcs_task >> [load_reviews_to_bigquery_task, load_courses_to_bigquery_task]
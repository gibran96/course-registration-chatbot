import sys

sys.path.append("..")


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.data_utils import upload_to_gcs
from scripts.extract_data import process_pdf_files
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}


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

    # Task to process PDFs and create CSVs
    process_pdfs = PythonOperator(
        task_id='process_pdfs',
        python_callable=process_pdf_files,
        provide_context=True,
    )

    # Task to upload processed files back to GCS
    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        provide_context=True,
    )

    load_reviews_to_bigquery_task = GCSToBigQueryOperator(
    task_id='load_to_bigquery',
    bucket=Variable.get('default_bucket_name'),
    source_objects=['processed_trace_data/reviews.csv'],
    destination_project_dataset_table=Variable.get('review_table_name'),
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    dag=dag,
    )

    load_courses_to_bigquery_task = GCSToBigQueryOperator(
    task_id='load_courses_to_bigquery',
    bucket=Variable.get('default_bucket_name'),
    source_objects=['processed_trace_data/courses.csv'],
    destination_project_dataset_table=Variable.get('course_table_name'),
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    dag=dag,
    )



    # Set task dependencies
    process_pdfs >> upload_to_gcs_task >> [load_reviews_to_bigquery_task, load_courses_to_bigquery_task]
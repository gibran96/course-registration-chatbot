from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_pipeline.dags.scripts.data_utils import upload_banner_data_to_gcs
from data_pipeline.dags.scripts.fetch_banner_data import get_courses_list, get_cookies, get_course_description, dump_to_csv
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
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

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
    
    get_cookies_task = PythonOperator(
        task_id='get_cookies_task',
        python_callable=get_cookies,
        provide_context=True,
        dag=dag
    )
    
    get_course_list_task = PythonOperator(
        task_id='get_course_list_task',
        python_callable=get_courses_list,
        op_args=[get_cookies_task.output],
        provide_context=True,
        dag=dag
    ),
    
    get_course_description_task = PythonOperator(
        task_id='get_course_description_task',
        python_callable=get_course_description,
        op_args=[get_cookies_task.output, get_course_list_task.output],
        provide_context=True,
        dag=dag
    ),
    
    dump_to_csv_task = PythonOperator(
        task_id='dump_to_csv_task',
        python_callable=dump_to_csv,
        op_args=[get_course_list_task.output],
        provide_context=True,
        dag=dag
    ),
    
    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_banner_data_to_gcs,
        provide_context=True,
        dag=dag
    )
    
    load_banner_data_to_bq_task = GCSToBigQueryOperator(
        task_id='load_banner_data_to_bq_task',
        bucket=Variable.get('default_bucket_name'),
        source_objects=['processed_trace_data/banner_course_data.csv'],
        destination_project_dataset_table=Variable.get('banner_table_name'),
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        dag=dag,
    )
    
    
    
    
    
    
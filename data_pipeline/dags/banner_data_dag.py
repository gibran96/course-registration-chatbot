from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.data_utils import upload_banner_data_to_gcs
from scripts.fetch_banner_data import get_courses_list, get_cookies, get_course_description, dump_to_csv, get_faculty_info, get_course_prerequisites
from scripts.extract_trace_data import process_pdf_files
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import Variable

from scripts.opt_fetch_banner_data import parallel_course_description, parallel_faculty_info, parallel_prerequisites

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
    'banner_dag_pipeline',
    default_args=default_args,
    description='Fetch course data from Banner and upload to GCS and BigQuery',
    schedule_interval='0 0 * * *',  # Daily at midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['banner', 'gcs'],
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
        op_kwargs={
            'cookie_output': "{{ task_instance.xcom_pull(task_ids='get_cookies_task') }}"
        },
        provide_context=True,
        dag=dag
    )
    
    get_faculty_info_parallel = PythonOperator(
        task_id='get_faculty_info_parallel',
        python_callable=parallel_faculty_info,
        provide_context=True,
        dag=dag
    )
    
    get_course_description_parallel = PythonOperator(
        task_id='get_course_description_parallel',
        python_callable=parallel_course_description,
        provide_context=True,
        dag=dag
    )
    
    get_prerequisites_parallel = PythonOperator(
        task_id='get_prerequisites_parallel',
        python_callable=parallel_prerequisites,
        provide_context=True,
        dag=dag
    )
    
    dump_to_csv_task = PythonOperator(
        task_id='dump_to_csv_task',
        python_callable=dump_to_csv,
        op_kwargs={
            'course_data': "{{ task_instance.xcom_pull(task_ids='get_prerequisites_parallel') }}"
        },
        provide_context=True,
        dag=dag
    )
    
    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_banner_data_to_gcs,
        provide_context=True,
        dag=dag
    )
    
    load_banner_data_to_bq_task = GCSToBigQueryOperator(
        task_id='load_banner_data_to_bq_task',
        bucket=Variable.get('default_bucket_name'),
        source_objects=['banner_data/banner_course_data.csv'],
        destination_project_dataset_table=Variable.get('banner_table_name'),
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        autodetect=None,
        dag=dag,
    )
    
    get_cookies_task >> get_course_list_task >> get_faculty_info_parallel >> get_course_description_parallel >> get_prerequisites_parallel >> dump_to_csv_task >> upload_to_gcs_task >> load_banner_data_to_bq_task
    
    
    
    
    
    
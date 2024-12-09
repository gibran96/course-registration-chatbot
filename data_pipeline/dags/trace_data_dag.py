from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.gcs.gcs_utils import get_unique_blobs, upload_to_gcs
from scripts.trace.extract_trace_data import (
    read_and_parse_pdf_files, 
    preprocess_data, 
    get_crn_list, 
    monitor_new_rows_and_trigger
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor, GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from airflow.models import Variable

from airflow.operators.email import EmailOperator

import logging
logging.basicConfig(level=logging.INFO)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['mlopsggmu@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Create DAG
with DAG(
    'pdf_processing_pipeline',
    default_args=default_args,
    description='Process PDFs from GCS and extract course review data',
    # every 5 months interval
    schedule_interval='0 0 1 */5 *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['pdf', 'processing', 'gcs'],
    max_active_runs=1
) as dag:

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
        python_callable=read_and_parse_pdf_files,
        provide_context=True,
        dag=dag
    )

    preproceess_pdfs = PythonOperator(
        task_id='preprocess_pdfs',
        python_callable=preprocess_data,
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
        source_objects=['processed_trace_data/reviews_preprocessed.csv'],
        destination_project_dataset_table=Variable.get('review_table_name'),
        write_disposition='WRITE_APPEND',
        autodetect=None,
        skip_leading_rows=1,
        dag=dag,
    )

    load_courses_to_bigquery_task = GCSToBigQueryOperator(
        task_id='load_courses_to_bigquery',
        bucket=Variable.get('default_bucket_name'),
        source_objects=['processed_trace_data/courses_preprocessed.csv'],
        destination_project_dataset_table=Variable.get('course_table_name'),
        write_disposition='WRITE_APPEND',
        autodetect=None,
        skip_leading_rows=1,
        dag=dag,
    )

        
    success_email_task = EmailOperator(
        task_id='success_email',
        to='mlopsggmu@gmail.com',
        subject='DAG pdf_processing_pipeline Succeeded',
        html_content="""<p>Dear User,</p>
                        <p>The DAG <strong>{{ dag.dag_id }}</strong> was copleted successfully on {{ ds }}.</p>
                        <p><strong>Execution Date:</strong> {{ execution_date }}</p>
                        <p>Please check the <a href="{{ task_instance.log_url }}">task logs</a> for more details.</p>
                        <br/><br/>
                        <p>Best regards,</p>
                        <p>Airflow Notifications</p>""",
        trigger_rule='all_success',
        dag=dag,
    )

    trigger_train_data_pipeline = PythonOperator(
        task_id='trigger_train_data_pipeline',
        python_callable=monitor_new_rows_and_trigger,
        provide_context=True,
        dag=dag
    )



    # Set task dependencies
    (
        get_crn_list_task 
        >> unique_blobs 
        >> process_pdfs 
        >> preproceess_pdfs 
        >> upload_to_gcs_task 
        >> load_courses_to_bigquery_task
        >> load_reviews_to_bigquery_task
        >> trigger_train_data_pipeline
        >> success_email_task
    )
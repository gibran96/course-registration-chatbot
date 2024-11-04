from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.data_utils import upload_banner_data_to_gcs
from scripts.fetch_banner_data import get_courses_list, get_cookies, dump_to_csv
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.models import Variable
from airflow.operators.email import EmailOperator

from scripts.opt_fetch_banner_data import parallel_course_description, parallel_faculty_info, parallel_prerequisites

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
        provide_context=True,
        dag=dag
    )
    
    get_faculty_info_task = PythonOperator(
        task_id='get_faculty_info_task',
        python_callable=parallel_faculty_info,
        provide_context=True,
        dag=dag
    )
    
    get_course_description_task = PythonOperator(
        task_id='get_course_description_task',
        python_callable=parallel_course_description,
        provide_context=True,
        dag=dag
    )
    
    get_prerequisites_task = PythonOperator(
        task_id='get_prerequisites_task',
        python_callable=parallel_prerequisites,
        provide_context=True,
        dag=dag
    )
    
    dump_to_csv_task = PythonOperator(
        task_id='dump_to_csv_task',
        python_callable=dump_to_csv,
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
    
    success_email_task = EmailOperator(
        task_id='success_email',
        to='mlopsggmu@gmail.com',
        subject='DAG banner_dag_pipeline Succeeded',
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
    
    (
        get_cookies_task
        >> get_course_list_task
        >> get_faculty_info_task
        >> get_course_description_task
        >> get_prerequisites_task
        >> dump_to_csv_task
        >> upload_to_gcs_task
        >> load_banner_data_to_bq_task
        >> success_email_task
    )
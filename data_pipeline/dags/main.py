from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow import configuration as conf
from airflow.operators.email import EmailOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'start_date': datetime.now(),
    'retries': 1,
}

dag = DAG(
    'Data_Pipeline',
    default_args=default_args,
    schedule_interval='0 0 1 * *',
    catchup=False,
    tags=['data-pipeline'],
    owner_links={"Dev_Team": "https://github.com/gibran96/course-registration-chatbot"}
)

# Task definitions
owner_task = BashOperator(
    task_id="task_using_linked_owner",
    bash_command="echo 'Owner: {{ dag.owner }}'",
    owner="Dev_Team",
    dag=dag
)

gcp_credentials = BashOperator(
    task_id="set_gcp_credentials",
    bash_command='gcloud auth activate-service-account --key-file /home/airflow/gcs/data/airflow-service-account-key.json && gcloud config set project course-registration-chatbot',
    dag=dag
)


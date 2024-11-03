from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.data_utils import upload_train_data_to_gcs  # Reusing existing utility
import logging
import logging
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

from scripts.bigquery_utils import (
    check_sample_count_from_bq,
    get_bq_data,
    perform_similarity_search,
    upload_gcs_to_bq
)
from scripts.data_processing import (
    get_initial_queries,
    generate_llm_response,
    upload_train_data_to_gcs
)
from scripts.data_utils import (
    upload_to_gcs
)       

logging.basicConfig(level=logging.INFO)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['mlopsggmu@gmail.com'],
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}
 
def trigger_dag_run(**context):
    task_status = context['ti'].xcom_pull(task_ids='check_sample_count_from_bq', key='task_status')
    if task_status == "stop_task":
        return "stop_task"
    trigger_dag_run = TriggerDagRunOperator(
        task_id='trigger_dag_run',
        trigger_dag_id=dag.dag_id,
    )
    trigger_dag_run.execute(context=context)
    return "generate_samples"


with DAG(
    'train_data_dag',
    default_args=default_args,
    description='Generate synthetic training data',
    schedule_interval=None, 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['pdf', 'banner', 'llm'],
    max_active_runs=1
) as dag:
    
    sample_count = PythonOperator(
        task_id='check_sample_count',
        python_callable=check_sample_count_from_bq,
        provide_context=True,
    )

    bq_data = PythonOperator(
        task_id='get_bq_data',
        python_callable=get_bq_data,
        provide_context=True,
    )

    initial_queries = PythonOperator(
        task_id='get_initial_queries',
        python_callable=get_initial_queries,
        provide_context=True,
    )


    similarity_search_results = PythonOperator(
        task_id='bq_similarity_search',
        python_callable=perform_similarity_search,
        provide_context=True,
    )

    llm_response = PythonOperator(
        task_id='generate_llm_response',
        python_callable=generate_llm_response,
        provide_context=True,
        dag=dag
    )

    upload_to_gcs = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_train_data_to_gcs,
        provide_context=True,
        dag=dag
    )

    load_to_bigquery_task = PythonOperator(
        task_id='upload_gcs_to_bq',
        python_callable=upload_gcs_to_bq,
        provide_context=True,
        dag=dag
    )

    trigger_dag_run = PythonOperator(
        task_id='trigger_dag_run',
        python_callable=trigger_dag_run,
        provide_context=True,
        dag=dag
    )
    
    success_email_task = EmailOperator(
        task_id='success_email',
        to='mlopsggmu@gmail.com',
        subject='DAG train_data_dag Succeeded',
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


    # Define task dependenciesa
    (
        sample_count 
        >> bq_data 
        >> initial_queries 
        >> similarity_search_results 
        >> llm_response 
        >> upload_to_gcs 
        >> load_to_bigquery_task 
        >> trigger_dag_run
        >> success_email_task
    )
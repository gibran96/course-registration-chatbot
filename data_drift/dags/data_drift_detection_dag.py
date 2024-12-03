from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import logging
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator

from scripts.bigquery_utils import get_train_queries_from_bq, get_new_queries, perform_similarity_search, upload_gcs_to_bq, move_data_from_user_table
from scripts.drift_detection import get_train_embeddings, get_test_embeddings, get_thresholds, detect_data_drift
from scripts.llm_utils import generate_llm_response

from airflow.operators.dagrun_operator import TriggerDagRunOperator

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

def trigger_train_data_dag(**context):

    drift_status = context['ti'].xcom_pull(task_ids='data_drift_detection', key='data_drift')
    logging.info(f"drift_status: {drift_status}")
    if drift_status == False:
        logging.info("No data drift detected. Not triggering train_data_dag")
        return False

    trigger_train_data_dag = TriggerDagRunOperator(
        task_id='trigger_train_data_dag',
        trigger_dag_id='train_data_dag',
        dag=dag
    )
    logging.info("Triggering train_data_dag")

    trigger_train_data_dag.execute(context=context)

    return True

 

with DAG(
    'data_drift_detection_dag',
    default_args=default_args,
    description='Detect data drift',
    schedule_interval=None, 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['pdf', 'banner', 'llm'],
    max_active_runs=1
) as dag:
    
    train_questions = PythonOperator(
        task_id='get_train_questions',
        python_callable=get_train_queries_from_bq,
        provide_context=True,
        dag=dag
    )

    new_questions = PythonOperator(
        task_id='get_test_questions',
        python_callable=get_new_queries,
        provide_context=True,
        dag=dag
    )

    train_embeddings = PythonOperator(
        task_id='get_train_embeddings',
        python_callable=get_train_embeddings,
        provide_context=True,
        dag=dag
    )

    test_embeddings = PythonOperator(
        task_id='get_test_embeddings',
        python_callable=get_test_embeddings,
        provide_context=True,
        dag=dag
    )

    thresholds = PythonOperator(
        task_id='get_thresholds',
        python_callable=get_thresholds,
        provide_context=True,
        dag=dag
    )

    data_drift = PythonOperator(
        task_id='data_drift_detection',
        python_callable=detect_data_drift,
        provide_context=True,
        dag=dag
    )

    similarity_search_results = PythonOperator(
        task_id='bq_similarity_search',
        python_callable=perform_similarity_search,
        provide_context=True,
        dag=dag
    )

    llm_response = PythonOperator(
        task_id='generate_llm_response',
        python_callable=generate_llm_response,
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
        python_callable=trigger_train_data_dag,
        provide_context=True,
        dag=dag
    )

    move_data_from_user_table = PythonOperator(
        task_id='move_data_from_user_table',
        python_callable=move_data_from_user_table,
        provide_context=True,
        dag=dag
    )


    success_email_task = EmailOperator(
        task_id='success_email',
        to='mlopsggmu@gmail.com',
        subject='DAG data_drift_pipeline Succeeded',
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


    # Define the task dependencies
    train_questions >> new_questions >> train_embeddings >> test_embeddings >> thresholds >> data_drift >> similarity_search_results >> llm_response >> load_to_bigquery_task >> trigger_dag_run >> success_email_task
    
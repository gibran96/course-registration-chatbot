from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import os
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import (
    SupervisedFineTuningTrainOperator,
)
from model_scripts.eval.model_evaluation import run_model_evaluation
from model_scripts.train.prepare_dataset import prepare_training_data
from model_scripts.model_deployment.endpoint_cleanup import delete_default_endpoint, deploy_new_model
from model_scripts.model_selection.best_model import compare_model
from airflow.models import Variable
import datetime
from model_scripts.bias.create_bias_detection_data import get_unique_profs, get_bucketed_queries, get_bq_data_for_profs, generate_responses, get_sentiment_score, generate_bias_report
from model_scripts.utils.data_utils import upload_to_gcs
from airflow.operators.email import EmailOperator


PROJECT_ID = os.environ.get("PROJECT_ID", "coursecompass")
REGION = Variable.get("REGION", "us-east1")
SOURCE_MODEL = Variable.get("SOURCE_MODEL","gemini-1.5-flash-002")
TRAIN_DATASET = Variable.get("TRAIN_DATASET","gs://mlops-data-7374/finetuning_data.jsonl")
TUNED_MODEL_DISPLAY_NAME = Variable.get("TUNED_MODEL_DISPLAY_NAME","course_registration_gemini_1_5_flash_002")

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
    "train_model_trigger_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    tags=["vertex-ai"],
    max_active_runs=1,
) as dag:
    
    prepare_training_data_task = PythonOperator(
        task_id="prepare_training_data",
        python_callable=prepare_training_data,
        provide_context=True,
    )

    upload_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
        provide_context=True,
    )

    sft_train_task = SupervisedFineTuningTrainOperator(
        task_id="sft_train_task",
        project_id=PROJECT_ID,
        location=REGION,
        source_model=SOURCE_MODEL,
        train_dataset=TRAIN_DATASET,
        tuned_model_display_name=TUNED_MODEL_DISPLAY_NAME + str(datetime.datetime.now().strftime("%Y%m%d%H%M%S")),
        epochs=1,
        adapter_size=1,
        learning_rate_multiplier=1.0,
    )

    model_evaluation_task = PythonOperator(
        task_id="model_evaluation_task",
        python_callable=run_model_evaluation,
        provide_context=True,
    )

    get_unique_profs_task = PythonOperator(
        task_id="get_unique_profs_task",
        python_callable=get_unique_profs,
        provide_context=True,
    )

    get_bucketed_queries_task = PythonOperator(
        task_id="get_bucketed_queries_task",
        python_callable=get_bucketed_queries,
        provide_context=True,
    )

    get_bq_data_for_profs_task = PythonOperator(
        task_id="get_bq_data_for_profs_task",
        python_callable=get_bq_data_for_profs,
        provide_context=True,
    )

    generate_responses_task = PythonOperator(
        task_id="generate_responses_task",
        python_callable=generate_responses,
        provide_context=True,
    )

    get_sentiment_score_task = PythonOperator(
        task_id="get_sentiment_score_task",
        python_callable=get_sentiment_score,
        provide_context=True,
    )

    generate_bias_report_task = PythonOperator(
        task_id="generate_bias_report_task",
        python_callable=generate_bias_report,
        provide_context=True,
    )

    delete_default_endpoint_task = PythonOperator(
        task_id="delete_default_endpoint",
        python_callable=delete_default_endpoint,
        provide_context=True,
    )

    compare_model_task = BranchPythonOperator(
        task_id="compare_model",
        python_callable=compare_model,
        provide_context=True,
    )

    deploy_new_model_task = PythonOperator(
        task_id="deploy_new_model",
        python_callable=deploy_new_model,
        provide_context=True,
    )

    end_dag_task = EmptyOperator(task_id="end_dag"                         
        )
    
    success_email_task = EmailOperator(
        task_id='success_email',
        to='mlopsggmu@gmail.com',
        subject='DAG train_eval_model_pipeline Succeeded',
        html_content="""<p>Dear User,</p>
                        <p>The DAG <strong>{{ dag.dag_id }}</strong> was copleted successfully on {{ ds }}.</p>
                        <p><strong>Execution Date:</strong> {{ execution_date }}</p>
                        <p>Please check the <a href="{{ task_instance.log_url }}">task logs</a> for more details.</p>
                        <br/><br/>
                        <p>Best regards,</p>
                        <p>Airflow Notifications</p>""",
        trigger_rule='one_success',
        dag=dag,
    )


    
    (
        prepare_training_data_task 
        >> upload_to_gcs_task 
        >> sft_train_task 
        >> model_evaluation_task 
        >> get_unique_profs_task 
        >> get_bucketed_queries_task 
        >> get_bq_data_for_profs_task 
        >> generate_responses_task
        >> get_sentiment_score_task 
        >> generate_bias_report_task
        >> delete_default_endpoint_task
        >> compare_model_task >> [deploy_new_model_task, end_dag_task]
    )
    end_dag_task >> success_email_task
    deploy_new_model_task >> success_email_task
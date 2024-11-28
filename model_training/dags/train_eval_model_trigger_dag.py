from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import (
    SupervisedFineTuningTrainOperator,
)
from model_scripts.eval.model_evaluation import run_model_evaluation
from model_scripts.train.prepare_dataset import prepare_training_data

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
    
with DAG(
    "train_model_trigger_dag",
    schedule_interval=None,
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    tags=["vertex-ai"],
    email_on_failure=True
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
        trigger_rule='all_success',
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
        >> success_email_task
    )
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from airflow.operators.email import EmailOperator
import os
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import (
    SupervisedFineTuningTrainOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator
)
from model_scripts.prepare_dataset import prepare_training_data
from model_scripts.data_utils import upload_to_gcs
from airflow.models import Variable
import datetime


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
        learning_multiplier_rate=1.0,
    )

    prepare_training_data_task >> upload_to_gcs_task >> sft_train_task

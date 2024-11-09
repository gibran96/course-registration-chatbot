from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import (
    SupervisedFineTuningTrainOperator,
)


sft_train_task = SupervisedFineTuningTrainOperator(
    task_id="sft_train_task",
    project_id=PROJECT_ID,
    location=REGION,
    source_model=SOURCE_MODEL,
    train_dataset=TRAIN_DATASET,
    tuned_model_display_name=TUNED_MODEL_DISPLAY_NAME,
)
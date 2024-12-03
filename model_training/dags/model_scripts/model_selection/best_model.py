import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from google.cloud import aiplatform
from datetime import datetime

PROJECT_ID = Variable.get("PROJECT_ID", "coursecompass")
REGION = Variable.get("EVAL_REGION", "us-east1")
BEST_MODEL_ID = Variable.get("best_existing_model_id")
BEST_MODEL_EXPERIMENT_ID = Variable.get("best_existing_model_experiment_id")
BEST_MODEL_EXPERIMENT_RUN_ID = Variable.get("best_existing_model_experiment_run_id")

def compare_current_model_with_best_model(current_model_experiment_run_id, current_model_experiment_id):
    aiplatform.init(project=PROJECT_ID, location=REGION)

    best_experiment_run = aiplatform.ExperimentRun(BEST_MODEL_EXPERIMENT_RUN_ID, experiment=BEST_MODEL_EXPERIMENT_ID)
    current_experiment_run = aiplatform.ExperimentRun(current_model_experiment_run_id, experiment=current_model_experiment_id)

    best_metrics = best_experiment_run.get_metrics()
    current_metrics = current_experiment_run.get_metrics()

    logging.info(f"Best model metrics: {best_metrics}")
    logging.info(f"Current model metrics: {current_metrics}")

    return best_metrics, current_metrics

def compare_model(**context):
    current_model_experiment_run_id = context["ti"].xcom_pull(task_ids="model_evaluation_task", key="experiment_run_name")
    current_model_experiment_id = context["ti"].xcom_pull(task_ids="model_evaluation_task", key="experiment_name")
    logging.info(f"Current model experiment run ID: {current_model_experiment_run_id}")
    logging.info(f"Current model experiment ID: {current_model_experiment_id}")
    best_metrics, current_metrics = compare_current_model_with_best_model(current_model_experiment_run_id, current_model_experiment_id)

    context["ti"].xcom_push(key="best_metrics", value=best_metrics)
    context["ti"].xcom_push(key="current_metrics", value=current_metrics)

    if current_metrics['bleu/mean'] >= best_metrics['bleu/mean'] and current_metrics['rouge_l_sum/mean'] >= best_metrics['rouge_l_sum/mean']:
        return 'deploy_new_model'
    else:
        return 'end_dag'
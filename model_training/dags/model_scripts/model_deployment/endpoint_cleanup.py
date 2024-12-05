
from airflow.models import Variable
import datetime
# from model_scripts.bias.create_bias_detection_data import get_unique_profs, get_bucketed_queries, get_bq_data_for_profs, generate_responses, get_sentiment_score, generate_bias_report
# from model_scripts.utils.data_utils import upload_to_gcs
from airflow.operators.email import EmailOperator
import vertexai
from google.cloud import bigquery
from vertexai.generative_models import GenerationConfig, GenerativeModel, HarmCategory, HarmBlockThreshold
from airflow.models import Variable
import re
import ast
import random
import string
import time
import logging
from random import uniform
from functools import wraps
from typing import Callable, Any
import pandas as pd
from google.cloud import aiplatform


PROJECT_ID = Variable.get("PROJECT_ID", "coursecompass")
REGION = Variable.get("REGION", "us-east1")


def delete_default_endpoint(**context):
    """
    Deletes the default endpoint created by Vertex AI.
    
    Args:
        endpoint_name (str): The name of the default endpoint to delete.
    """
    endpoint_name = context["ti"].xcom_pull(task_ids="sft_train_task", key="tuned_model_endpoint_name")
    endpoint_id = endpoint_name.split("/")[-1]
    aiplatform.init(project=PROJECT_ID, location=REGION)
    endpoint = aiplatform.Endpoint(endpoint_name=endpoint_id)
    logging.info(f"Deleting endpoint {endpoint_name}")
    endpoint.delete(force=True)
    logging.info(f"Endpoint {endpoint_name} deleted successfully.")


def deploy_new_model(**context):
    """
    Deploys a new model to the default endpoint.
    
    Args:
        model_name (str): The name of the model to deploy.
        endpoint_name (str): The name of the default endpoint.
    """
    model_name = context["ti"].xcom_pull(task_ids="sft_train_task", key="tuned_model_name").split("/")[-1]
    endpoint_name = Variable.get("DEFAULT_ENDPOINT")
    aiplatform.init(project=PROJECT_ID, location=REGION)
    endpoint = aiplatform.Endpoint(endpoint_name=endpoint_name)
    model = aiplatform.Model(model_name=model_name)
    logging.info(f"Deploying model {model_name} to endpoint {endpoint_name}")
    endpoint.deploy(model=model, traffic_percentage=100)
    logging.info(f"Model {model_name} deployed successfully to endpoint {endpoint_name}")
    Variable.set("best_existing_model_id", model_name)
    Variable.set("best_existing_model_experiment_id", context["ti"].xcom_pull(task_ids="model_evaluation_task", key="experiment_name"))
    Variable.set("best_existing_model_experiment_run_id", context["ti"].xcom_pull(task_ids="model_evaluation_task", key="experiment_run_name"))

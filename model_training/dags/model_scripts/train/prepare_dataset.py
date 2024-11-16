from model_training.dags.model_scripts.constants.prompts import SYSTEM_INSTRUCTION, INSTRUCTION_PROMPT
from model_scripts.config import PROJECT_ID, DATASET_ID, TABLE_ID
from google.cloud import bigquery
import pandas as pd
import json
import logging
import os
from sklearn.model_selection import train_test_split
from airflow.models import Variable


logging.basicConfig(level=logging.INFO)

def init_bq_client(location, project):
    """
    Initializes a BigQuery client.

    Args:
        location (str): The location of the BigQuery dataset.
        project (str): The project ID of the BigQuery dataset.

    Returns:
        bigquery.Client: The BigQuery client object.
    """
    client = bigquery.Client(location=location, project=project)
    logging.info(f"Initialized BigQuery client with location: {location}, project: {project}")
    return client


def get_training_data(bigquery_client):
    """
    Retrieves data from BigQuery to train the LLM model.

    Args:
        bigquery_client (bigquery.Client): The BigQuery client object.

    Returns:
        bigquery.QueryJob: The BigQuery query job object.
    """
    query_job = bigquery_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` LIMIT {Variable.get('TRAINING_DATA_LIMIT', 50)}")
    logging.info(f"Selecting data from {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    return query_job

def extract_training_data(query_job):
    """
    Extracts the training data from the query job object.

    Args:
        query_job (bigquery.QueryJob): The BigQuery query job object.

    Returns:
        dict: The extracted data in the format of a dictionary with keys 'query', 'context', and 'response'.
    """
    data = {
        'query': [],
        'context': [],
        'response': []
    }
    for res in query_job:
        data['query'].append(res['question'])
        data['context'].append(res['context'])
        data['response'].append(res['response'])
    logging.info("Getting query results")
    logging.info(f"{len(data['query'])} results found")

    return data

def clean_and_filter_Data(data):
    """
    Cleans and filters the data from the query results.

    Args:
        data (dict): The extracted data in the format of a dictionary with keys 'query', 'context', and 'response'.

    Returns:
        tuple: A tuple of two pandas DataFrames, the first one representing the training set and the second one representing the test set.
    """
    df = pd.DataFrame(data)
    df = df.dropna()
    train_df, test_df = train_test_split(df, test_size=0.1, random_state=42)
    logging.info("Filtering data")
    return train_df, test_df


def format_data(df):
    """
    Formats the training data into the jsonl format expected by the Vertex AI SDK.

    Args:
        df (pd.DataFrame): The training data in the format of a pandas DataFrame with columns 'query', 'context', and 'response'.

    Returns:
        str: The formatted data as a string in the jsonl format.
    """
    jsonl_data = []
    for _, row in df.iterrows():
        json_item = {
            "systemInstruction": {
                "role": "system",
                "parts": [
                    {
                        "text": SYSTEM_INSTRUCTION
                        }
                    ]
                },
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "text": INSTRUCTION_PROMPT.format(query=row['query'], content=row['context'])
                            }
                        ]
                    },
                {
                    "role": "model",
                    "parts": [
                        {
                            "text": row['response']
                            }
                        ]
                    }
                ]
            }
        jsonl_data.append(json.dumps(json_item))
    return "\n".join(jsonl_data)

def format_eval_data(df):
    """
    Formats evaluation data from a DataFrame into a JSONL string.

    Args:
        df (pd.DataFrame): The evaluation data containing columns 'context', 'query', and 'response'.

    Returns:
        str: The formatted data as a JSONL string, where each line is a JSON object with
             'context', 'instruction', and 'reference' keys corresponding to the DataFrame columns.
    """
    json_data = []

    for _, row in df.iterrows():
        json_item = {
            "context": row['context'],
            "instruction": row['query'],
            "reference": row['response']
        }
        json_data.append(json.dumps(json_item))

    return "\n".join(json_data)

def prepare_training_data(**context):
    """
    Prepares training data by fetching it from BigQuery, extracting relevant columns,
    filtering out bad data, formatting it into a JSONL string, and saving it to a file.

    Args:
        **context: The Airflow context dictionary containing task instance (ti) and other metadata.

    Returns:
        None
    """
    bigquery_client = init_bq_client('us-east1', PROJECT_ID)

    data = get_training_data(bigquery_client)
    data = extract_training_data(data)
    train_df, test_df = clean_and_filter_Data(data)
    training_data = format_data(train_df)
    test_data = format_eval_data(test_df)
    os.makedirs("tmp", exist_ok=True)
    with open("tmp/finetuning_data.jsonl", "w") as f:
        f.write(training_data)
    with open("tmp/test_data.jsonl", "w") as f:
        f.write(test_data)

    logging.info("Training data prepared and saved to tmp/finetuning_data.jsonl")
    logging.info("Test data prepared and saved to tmp/test_data.jsonl")
    context['ti'].xcom_push(key='training_data_file_path', value="tmp/finetuning_data.jsonl")
    context['ti'].xcom_push(key='test_data_file_path', value="tmp/test_data.jsonl")
    context['ti'].xcom_push(key='test_data', value=format_eval_data(test_df))

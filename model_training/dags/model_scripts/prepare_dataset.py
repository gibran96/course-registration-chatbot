from model_scripts.prompts import SYSTEM_INSTRUCTION, INSTRUCTION_PROMPT
from model_scripts.config import PROJECT_ID, DATASET_ID, TABLE_ID
from google.cloud import bigquery
import pandas as pd
import json
import logging
import os
from sklearn.model_selection import train_test_split


logging.basicConfig(level=logging.INFO)

def init_bq_client(location, project):
    client = bigquery.Client(location=location, project=project)
    logging.info(f"Initialized BigQuery client with location: {location}, project: {project}")
    return client


def get_training_data(bigquery_client):

    query_job = bigquery_client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` LIMIT 50")
    logging.info(f"Selecting data from {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    return query_job

def extract_training_data(query_job):
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
    df = pd.DataFrame(data)
    df = df.dropna()
    train_df, test_df = train_test_split(df, test_size=0.1, random_state=42)
    logging.info("Filtering data")
    return train_df, test_df


def format_data(df):
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
    json_data = {}
    json_data['context'] = []
    json_data['instruction'] = []
    json_data['reference'] = []

    for _, row in df.iterrows():
        json_data['context'].append(row['context'])
        json_data['instruction'].append(row['query'])
        json_data['reference'].append(row['response'])

    return json_data

def prepare_training_data(**context):
    bigquery_client = init_bq_client('us-east1', PROJECT_ID)

    data = get_training_data(bigquery_client)
    data = extract_training_data(data)
    train_df, test_df = clean_and_filter_Data(data)
    training_data = format_data(train_df)
    test_data = format_data(test_df)
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

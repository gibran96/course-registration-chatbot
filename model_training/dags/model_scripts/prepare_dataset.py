from model_scripts.prompts import SYSTEM_INSTRUCTION, INSTRUCTION_PROMPT
from model_scripts.config import PROJECT_ID, DATASET_ID, TABLE_ID
from google.cloud import bigquery
import pandas as pd
import json
import logging

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
    logging.info("Filtering data")
    return df


def format_training_data(df):
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

def prepare_training_data(**context):
    bigquery_client = init_bq_client('us-east1', PROJECT_ID)

    data = get_training_data(bigquery_client)
    data = extract_training_data(data)
    data = clean_and_filter_Data(data)
    training_data = format_training_data(data)
    with open("tmp/finetuning_data.jsonl", "w") as f:
        f.write(training_data)

    logging.info("Training data prepared and saved to tmp/finetuning_data.jsonl")
    context['ti'].xcom_push(key='training_data_file_path', value="tmp/finetuning_data.jsonl")
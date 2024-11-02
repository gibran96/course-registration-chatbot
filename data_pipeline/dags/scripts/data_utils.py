import logging
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable
import os
import string


def remove_punctuation(text):
    punts = string.punctuation
    new_text = ''.join(e for e in text if e not in punts)
    return new_text

def upload_train_data_to_gcs():
    bucket_name = Variable.get('default_bucket_name')
    output_path = '/tmp'

    gcs_hook = GCSHook()
    filename = 'llm_train_data.pq'
    local_path = f"{output_path}/{filename}"
    gcs_path = f"processed_trace_data/{filename}"

    if os.path.exists(local_path):
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=gcs_path,
            filename=local_path
        )
        logging.info(f"Uploaded {filename} to GCS")

def upload_to_gcs(**context):
    bucket_name = context['dag_run'].conf.get('bucket_name', Variable.get('default_bucket_name'))
    output_path = context['dag_run'].conf.get('output_path', '/tmp/processed_data')
    
    gcs_hook = GCSHook()
    
    # Upload all CSV files
    for filename in ['reviews_preprocessed.pq', 'courses_preprocessed.pq', 'question_mapping.csv']:
        local_path = f"{output_path}/{filename}"
        gcs_path = f"processed_trace_data/{filename}"
        
        if os.path.exists(local_path):
            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=gcs_path,
                filename=local_path
            )
            logging.info(f"Uploaded {filename} to GCS")
            
def upload_banner_data_to_gcs(**context):
    bucket_name = context['dag_run'].conf.get('bucket_name', Variable.get('default_bucket_name'))
    output_path = context['dag_run'].conf.get('output_path', '/tmp/banner_data')
    
    gcs_hook = GCSHook()
    
    # Upload the CSV file
    local_path = f"{output_path}/banner_course_data.csv"
    gcs_path = "banner_data/banner_course_data.csv"
    
    if os.path.exists(local_path):
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=gcs_path,
            filename=local_path
        )
        logging.info(f"Uploaded banner_course_data.csv to GCS")
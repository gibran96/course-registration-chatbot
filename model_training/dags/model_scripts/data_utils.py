from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os
import logging

def upload_to_gcs(**context):
    bucket_name = context['dag_run'].conf.get('bucket_name', Variable.get('default_bucket_name'))
    filename = context['ti'].xcom_pull(task_ids='prepare_training_data', key='training_data_file_path')    
    gcs_hook = GCSHook()
    
    # Upload all CSV files
    local_path = filename
    gcs_path = f"{filename.split('/')[-1]}"
    
    if os.path.exists(local_path):
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=gcs_path,
            filename=local_path
        )
        logging.info(f"Uploaded {filename} to GCS")

        
import logging

from google.cloud.storage import storage
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable
import os
import string


def remove_punctuation(text):
    punts = string.punctuation
    new_text = ''.join(e for e in text if e not in punts)
    return new_text


def upload_train_data_to_gcs():
    try:
        bucket_name = Variable.get('default_bucket_name')
        output_path = '/tmp'
        filename = 'llm_train_data.pq'
        local_path = f"{output_path}/{filename}"
        gcs_path = f"processed_trace_data/{filename}"
        
        # Verify bucket name
        if not bucket_name:
            logging.error("Bucket name is not set in Airflow variables.")
            return

        # Initialize GCS Hook
        gcs_hook = GCSHook()

        # Check if file exists
        if os.path.exists(local_path):
            # Try uploading the file
            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=gcs_path,
                filename=local_path
            )
            logging.info(f"Uploaded {filename} to GCS at {gcs_path}")
        else:
            logging.warning(f"File {local_path} does not exist.")
    except Exception as e:
        logging.error(f"Failed to upload file to GCS: {str(e)}")

def upload_to_gcs(**context):
    bucket_name = context['dag_run'].conf.get('bucket_name', Variable.get('default_bucket_name'))
    output_path = context['dag_run'].conf.get('output_path', '/tmp/processed_data')
    
    gcs_hook = GCSHook()
    
    # Upload all CSV files
    for filename in ['reviews_preprocessed.csv', 'courses_preprocessed.csv', 'question_mapping.csv']:
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


def get_unique_blobs(**context):
    """
    Retrieves unique blobs from a Google Cloud Storage bucket that are not present in the provided CRN list.

    Args:
        **context: A dictionary containing context information for the DAG run. It should include:
            - 'dag_run': The current DAG run object, which should have a 'conf' attribute containing the configuration.
            - 'ti': The task instance object, which should have an 'xcom_pull' method to retrieve XCom values.

    Returns:
        list: A list of unique blob names (without the '.pdf' extension) that are not present in the CRN list.

    XCom Push:
        - key: 'unique_blobs'
        - value: The list of unique blob names.
    """
    bucket_name = context['dag_run'].conf.get('bucket_name', Variable.get('default_bucket_name'))
    all_gcs_crns = context['ti'].xcom_pull(task_ids='get_crn_list', key='crn_list')

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix='course_review_dataset/')

    unique_blobs = []
    # blob_names = [blob.name.split('/')[-1].replace('.pdf', '') for blob in blobs]
    for blob in blobs:
        blob_name = blob.name.split('/')[-1].replace('.pdf', '')
        if blob_name not in all_gcs_crns:
            unique_blobs.append(blob_name)

    context['ti'].xcom_push(key='unique_blobs', value=unique_blobs)

    return unique_blobs
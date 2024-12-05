from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os
import logging

def upload_to_gcs(**context):
    """
    Uploads the training and test data files to a Google Cloud Storage (GCS) bucket.

    This function retrieves file paths for training and test data from XCom, checks if the files exist locally,
    and uploads them to the specified GCS bucket. After successful upload, it logs the uploads and pushes the 
    GCS path of the test data file to XCom.

    Args:
        context (dict): A dictionary containing context information passed from the DAG run. Expected keys include:
            - 'dag_run': The DAG run object, which should contain 'conf' with 'bucket_name'.
            - 'ti': The task instance object, used to pull XCom data.

    Raises:
        FileNotFoundError: If any of the specified files do not exist locally.
    """
    bucket_name = context['dag_run'].conf.get('bucket_name', Variable.get('default_bucket_name'))
    filename = context['ti'].xcom_pull(task_ids='prepare_training_data', key='training_data_file_path')    
    test_file_name = context['ti'].xcom_pull(task_ids='prepare_training_data', key='test_data_file_path')
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

        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=test_file_name.split('/')[-1],
            filename=test_file_name
        )
        logging.info(f"Uploaded {test_file_name} to GCS")
        uploaded_file_path = f"gs://{bucket_name}/{test_file_name.split('/')[-1]}"
        context['ti'].xcom_push(key='uploaded_test_file_path', value=uploaded_file_path)

def upload_eval_data_to_gcs(**context):
    """
    Uploads the generated evaluation data to GCS.

    This function retrieves file paths for male and female evaluation data from XCom, checks if the files exist locally,
    and uploads them to the specified GCS bucket. After successful upload, it logs the uploads and pushes the GCS path
    of the evaluation data files to XCom.

    Args:
        context (dict): A dictionary containing context information passed from the DAG run. Expected keys include:
            - 'dag_run': The DAG run object, which should contain 'conf' with 'bucket_name'.
            - 'ti': The task instance object, used to pull XCom data.

    Raises:
        FileNotFoundError: If any of the specified files do not exist locally.
    """
    bucket_name = context['dag_run'].conf.get('bucket_name', Variable.get('default_bucket_name'))
    male_file_name = context['ti'].xcom_pull(task_ids='generate_eval_data', key='male_data_eval_path')
    female_file_name = context['ti'].xcom_pull(task_ids='generate_eval_data', key='female_data_eval_path')
    gcs_hook = GCSHook()
    for filename in [male_file_name, female_file_name]:
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

    context['ti'].xcom_push(key='male_data_eval_path_gcs', value=f'gs://{bucket_name}/{male_file_name.split("/")[-1]}')
    context['ti'].xcom_push(key='female_data_eval_path_gcs', value=f'gs://{bucket_name}/{female_file_name.split("/")[-1]}')

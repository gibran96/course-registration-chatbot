import logging
import os
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import storage

def upload_train_data_to_gcs(**context):
    """
    Upload the Parquet file with the generated LLM training data to GCS.

    This function checks if the file exists locally and if so, tries to upload it
    to the specified GCS bucket and path. If the upload is successful, it returns
    the string 'generate_samples' to trigger the next task in the DAG. If the
    file does not exist, it logs a warning. If there is an error during the upload
    process, it logs an error and stops the DAG.

    Parameters
    ----------
    **context : dict
        A dictionary containing context information passed from the DAG run.
        Expected keys include:
            - 'ti': The task instance object, used to pull XCom data.

    Returns
    -------
    str or None
        Either the string 'generate_samples' to trigger the next task in the DAG
        or None if the file does not exist.
    """
    task_status = context['ti'].xcom_pull(task_ids='check_sample_count', key='task_status')
    logging.info(f"task_status: {task_status}")
    if task_status == "stop_task":
        return "stop_task"
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

            return 'generate_samples'
        else:
            logging.warning(f"File {local_path} does not exist.")
    except Exception as e:
        logging.error(f"Failed to upload file to GCS: {str(e)}")


def upload_to_gcs(**context):
    """
    Uploads the preprocessed CSV files from the local machine to a GCS bucket.
    
    Args:
        context: A dictionary containing context information passed from the DAG run. Expected keys include:
            - 'dag_run': The DAG run object, which should contain 'conf' with 'bucket_name' and 'output_path'.
    """
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

            # Remove the local file
            os.remove(local_path)
            
def upload_banner_data_to_gcs(**context):
    """
    Uploads the banner course data CSV file from the local file system to a specified Google Cloud Storage bucket.

    This function checks for the existence of the CSV file in the local directory and uploads it to the designated
    GCS bucket using the provided path configuration from the DAG run context.

    Parameters
    ----------
    **context : dict
        A dictionary containing context information passed from the DAG run. Expected keys include:
            - 'dag_run': The DAG run object, which should contain 'conf' with 'bucket_name' and 'output_path'.

    Logs
    ----
    Logs an info message upon successful upload of the CSV file to GCS.
    """
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
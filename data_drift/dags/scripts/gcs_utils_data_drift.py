from airflow.models import Variable
import os
import logging
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
    drift_status = context['ti'].xcom_pull(task_ids='data_drift_detection', key='data_drift')
    logging.info(f"task_status: {drift_status}")
    if drift_status == False:
        return False
    try:
        bucket_name = Variable.get('default_bucket_name')
        output_path = '/tmp'
        filename = 'llm_train_data_drift.pq'
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

            return True
        else:
            logging.warning(f"File {local_path} does not exist.")
    except Exception as e:
        logging.error(f"Failed to upload file to GCS: {str(e)}")
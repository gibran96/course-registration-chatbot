from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sensors.external_task import ExternalTaskSensor
import json
import logging
import os
from model_scripts.model_evaluation import run_evaluation

# Get environment variables and configurations
PROJECT_ID = os.environ.get("PROJECT_ID", "coursecompass")
REGION = Variable.get("REGION", "us-east1")
SOURCE_MODEL = Variable.get("SOURCE_MODEL", "gemini-1.5-flash-002")


def get_latest_model_name(**context):
    """Get the model name from the triggering DAG"""
    try:
        model_name = context['ti'].xcom_pull(task_ids='sft_train_task', key='tuned_model_name')
        if not model_name:
            raise ValueError("No model name provided in DAG configuration")
            
        context['task_instance'].xcom_push(key='model_name', value=model_name)
        return model_name
    except Exception as e:
        logging.error(f"Error getting model name: {e}")
        raise


def save_evaluation_results(**context):
    """Save evaluation results to both GCS and local storage"""
    try:
        results = context['task_instance'].xcom_pull(task_ids='run_evaluation_task')
        if not results:
            raise ValueError("No evaluation results found")
            
        # Format results with timestamp and model info
        formatted_results = {
            'timestamp': datetime.now().isoformat(),
            'model_name': context['task_instance'].xcom_pull(task_ids='get_latest_model_task'),
            'metrics': results,
            'training_dag_id': context['dag_run'].conf.get('training_dag_id'),
            'training_run_id': context['dag_run'].conf.get('training_run_id')
        }
        
        # Save locally
        os.makedirs('tmp/eval_results', exist_ok=True)
        local_path = f"tmp/eval_results/eval_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(local_path, 'w') as f:
            json.dump(formatted_results, f, indent=2)
            
        # Upload to GCS
        bucket_name = context['dag_run'].conf.get('bucket_name', Variable.get('default_bucket_name'))
        gcs_hook = GCSHook()
        gcs_path = f"evaluation_results/eval_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=gcs_path,
            filename=local_path
        )
        
        # Log results summary
        logging.info(f"Evaluation Results Summary:")
        for metric, value in results.items():
            logging.info(f"{metric}: {value:.4f}")
            
    except Exception as e:
        logging.error(f"Error saving evaluation results: {e}")
        raise

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

with DAG(
    'model_evaluation_dag',
    default_args=default_args,
    description='Model evaluation pipeline for Course Compass',
    schedule_interval=None,  
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['evaluation']
) as dag:
    
    # Wait for training DAG to complete
    # wait_for_training = ExternalTaskSensor(
    #     task_id='wait_for_training',
    #     external_dag_id='train_model_trigger_dag',
    #     external_task_id='sft_train_task',
    #     timeout=7200,  # 2 hour timeout
    #     mode='reschedule'
    # )
    
    # Get latest model name
    get_latest_model = PythonOperator(
        task_id='get_latest_model_task',
        python_callable=get_latest_model_name,
        provide_context=True
    )
    
    # Run evaluation
    run_evaluation_task = PythonOperator(
        task_id='run_evaluation_task',
        python_callable=run_evaluation,
        provide_context=True
    )
    
    # Save results
    save_results = PythonOperator(
        task_id='save_results_task',
        python_callable=save_evaluation_results,
        provide_context=True
    )
    
    # Define task dependencies
    get_latest_model >> run_evaluation_task >> save_results
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Your existing DAG definition
with DAG(
    'pdf_processing_pipeline',
    default_args=default_args,
    description='Process PDFs from GCS and extract course review data',
    schedule_interval='0 0 * * *',  # Daily at midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['pdf', 'processing', 'gcs'],
    max_active_runs=1
) as dag:
    
    # Other existing tasks here...

    # Task to create the embedding model
    create_embedding_model_task = BigQueryInsertJobOperator(
        task_id='create_embedding_model_task',
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE MODEL `}`
                    REMOTE WITH CONNECTION `us.vector_ai`
                    OPTIONS (ENDPOINT = 'text-embedding-004');
                """,
                "useLegacySql": False,
            }
        },
        dag=dag
    )

    # Task to generate embeddings and store them in a table
    generate_embeddings_task = BigQueryInsertJobOperator(
        task_id='generate_embeddings_task',
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `cogent-summer-437922-h2.1.course_embeddings` AS
                    SELECT * FROM ML.GENERATE_EMBEDDING(
                        MODEL `1.embedding_model`,
                        (
                            SELECT *, 
                            courseDescription || " " || courseTitle || " " || subjectCourse || " " || facultyName as content
                            FROM `cogent-summer-437922-h2.1.course_data`
                        )
                    );
                """,
                "useLegacySql": False,
            }
        },
        dag=dag
    )

    # Set task dependencies
    create_embedding_model_task >> generate_embeddings_task

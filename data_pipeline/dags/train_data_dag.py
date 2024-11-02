from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models import Variable
from google.cloud import storage, bigquery
import vertexai.generative_models
from scripts.data_utils import upload_to_gcs  # Reusing existing utility
from scripts.fetch_banner_data import get_cookies, get_courses_list, get_course_description
import pandas as pd
import numpy as np
from scripts.seed_data import query_templates, topics, seed_query_list
import logging
import random
import vertexai

from vertexai.generative_models import GenerativeModel, HarmCategory, HarmBlockThreshold, GenerationConfig

PROJECT_ID = "coursecompass"
vertexai.init(project=PROJECT_ID, location="us-central1")

client_model = GenerativeModel(model_name="gemini-1.5-flash-002")

logging.basicConfig(level=logging.INFO)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

TARGET_SAMPLE_COUNT = 500


def get_llm_response(**context):
    input_prompt = context['ti'].xcom_pull(task_ids='generate_samples')
    res = client_model.generate_content(
            input_prompt,
            safety_settings={
                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
            },
            generation_config=GenerationConfig(
                max_output_tokens=1024,
                temperature=0.7,
            ),
        ).content

    context['ti'].xcom_push(key='llm_response', value=res)

    return res

def check_sample_count_from_bq(**context):
    client = bigquery.Client()
    table_id = Variable.get("train_data_table_name")
    
    query = f"SELECT COUNT(*) AS sample_count FROM `{table_id}`"
    result = client.query(query).result()
    sample_count = list(result)[0]["sample_count"]

    
    if sample_count >= TARGET_SAMPLE_COUNT:
        logging.info(f"Target sample count ({TARGET_SAMPLE_COUNT}) reached in BigQuery. Ending DAG run.")
        context['ti'].xcom_push(key='task_status', value="stop_task")
        return "stop_task"
    else:
        logging.info(f"Current sample count: {sample_count}. Proceeding with sample generation.")
        context['ti'].xcom_push(key='task_status', value="generate_samples")
        return "generate_samples"


def get_initial_queries(**context):

    task_status = context['ti'].xcom_pull(task_ids='check_sample_count_from_bq', key='task_status')
    if task_status == "stop_task":
        return "stop_task"
    else:

        course_list = context['ti'].xcom_pull(task_ids='get_bq_data', key='course_list')
        prof_list = context['ti'].xcom_pull(task_ids='get_bq_data', key='prof_list')

        col_names = ['topic']
        selected_col = random.choice(col_names)

        query_subset = [query for query in seed_query_list if selected_col in query]

        if selected_col == 'topic':
            topic = random.choice(topics)
            queries = [query.format(topic=topic) for query in query_subset]
        elif selected_col == 'course_name':
            course_name = random.choice(course_list)
            queries = [query.format(course_name=course_name) for query in query_subset]
        elif selected_col == 'professor_name':
            professor_name = random.choice(prof_list)
            queries = [query.format(professor_name=professor_name) for query in query_subset]

        context['ti'].xcom_push(key='initial_queries', value=queries)
        logging.info(f'Initial queries: {len(queries)}' )
        logging.info(queries)
        return "generate_samples"


def get_bq_data(**context):
    sample_count = context['ti'].xcom_pull(task_ids='check_sample_count', key='task_status')
    if sample_count == "stop_task":
        return "stop_task"
    else:
        client = bigquery.Client()
        
        # Query for professors
        prof_query = """
            SELECT DISTINCT faculty_name
            FROM `{}`""".format(Variable.get('banner_table_name'))
        
        # Query for courses
        course_query = """
            SELECT DISTINCT course_title
            FROM `{}`""".format(Variable.get('banner_table_name'))
        
        # Execute the queries and retrieve results
        prof_list = list(set(row["faculty_name"] for row in client.query(prof_query).result()))
        course_list = list(set(row["course_title"] for row in client.query(course_query).result()))
        
        # Push results to XCom
        context['ti'].xcom_push(key='prof_list', value=prof_list)
        context['ti'].xcom_push(key='course_list', value=course_list)
        return "generate_samples"


def perform_similarity_search(**context):

    task_status = context['ti'].xcom_pull(task_ids='check_sample_count_from_bq', key='task_status')
    if task_status == "stop_task":
        return "stop_task"
    queries = context['ti'].xcom_pull(task_ids='get_initial_queries', key='initial_queries')

    client = bigquery.Client()
    query_response = {}

    for query in queries:
        bq_query = """
            WITH query_embedding AS (
            SELECT ml_generate_embedding_result
            FROM ML.GENERATE_EMBEDDING(
                MODEL `coursecompass.mlopsdataset.embeddings_model`,
                (SELECT @query AS content)
            )
        )
        SELECT base.*
        FROM VECTOR_SEARCH(
            (
                SELECT *
                FROM `coursecompass.mlopsdataset.banner_data_embeddings`
                WHERE ARRAY_LENGTH(ml_generate_embedding_result) = 768
            ),
            'ml_generate_embedding_result',
            TABLE query_embedding,
            distance_type => 'COSINE',
            top_k => 10,
            options => '{"use_brute_force": true}'
        );
        """

        query_params = [
            bigquery.ScalarQueryParameter("query", "STRING", query)
        ]

        job_config = bigquery.QueryJobConfig(
            query_parameters=query_params
        )
        query_job = client.query(bq_query, job_config=job_config)

        results = query_job.result()
        results = [row.crn for row in results]

        query_response[query] = results

        logging.info(f"Similarity search results for query '{query}': {','.join(results)}")
   
    context['ti'].xcom_push(key='similarity_results', value=query_response)
    return "generate_samples"

def generate_llm_response(**context):
    pass


def process_llm_output(**context):
    llm_results = context['task_instance'].xcom_pull(key='llm_results')
    
    # Prepare data for BigQuery
    processed_results = []
    for result in llm_results:
        processed_results.append({
            'crn': result['crn'],
            'course_name': result['course_name'],
            'professor': result['professor'],
            'summary': result['analysis']['summary'],
            'strengths': result['analysis']['strengths'],
            'improvements': result['analysis']['improvements'],
            'assessment': result['analysis']['assessment'],
            'source_pdf': result['source_pdf'],
            'confidence_score': result['confidence_score'],
            'analysis_timestamp': datetime.now().isoformat()
        })
    
    # Save to temporary CSV for BigQuery upload
    df = pd.DataFrame(processed_results)
    df.to_csv('/tmp/llm_analysis_results.csv', index=False)
    
    # Upload to GCS
    upload_to_gcs(
        bucket_name=Variable.get('default_bucket_name'),
        source_path='/tmp/llm_analysis_results.csv',
        destination_path='llm_analysis/results.csv'
    )
    
    return processed_results

with DAG(
    'train_data_dag',
    default_args=default_args,
    description='Generate synthetic training data',
    schedule_interval='0 0 * * *',  # Daily at midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['pdf', 'banner', 'llm'],
    max_active_runs=1
) as dag:
    
    sample_count = PythonOperator(
        task_id='check_sample_count',
        python_callable=check_sample_count_from_bq,
        provide_context=True,
    )

    bq_data = PythonOperator(
        task_id='get_bq_data',
        python_callable=get_bq_data,
        provide_context=True,
    )

    initial_queries = PythonOperator(
        task_id='get_initial_queries',
        python_callable=get_initial_queries,
        provide_context=True,
    )


    similarity_search_results = PythonOperator(
        task_id='bq_similarity_search',
        python_callable=perform_similarity_search,
        provide_context=True,
    )

    # # Generate LLM responses
    # generate_llm_response_task = PythonOperator(
    #     task_id='generate_llm_response',
    #     python_callable=generate_llm_response,
    #     provide_context=True,
    # )

    # # Process LLM output
    # process_output_task = PythonOperator(
    #     task_id='process_llm_output',
    #     python_callable=process_llm_output,
    #     provide_context=True,
    # )

    # # Load results to BigQuery
    # load_to_bigquery_task = GCSToBigQueryOperator(
    #     task_id='load_to_bigquery',
    #     bucket=Variable.get('default_bucket_name'),
    #     source_objects=['llm_analysis/results.csv'],
    #     destination_project_dataset_table=Variable.get('train_data_table_name'),
    #     write_disposition='WRITE_APPEND',
    #     autodetect=True,
    #     skip_leading_rows=1,
    # )


    # Define task dependenciesa
    sample_count >> bq_data >> initial_queries >> similarity_search_results
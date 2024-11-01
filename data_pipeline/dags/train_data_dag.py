from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models import Variable
from google.cloud import storage, bigquery
from scripts.data_utils import upload_to_gcs  # Reusing existing utility
from scripts.fetch_banner_data import get_cookies, get_courses_list, get_course_description
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from data_pipeline.data.seed_data import query_templates, topics, seed_query_list
import openai  # Assuming OpenAI for LLM
import logging
import json
import random

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

def check_sample_count_from_bq(**context):
    client = bigquery.Client()
    table_id = Variable.get("llm_results_table")
    
    query = f"SELECT COUNT(*) AS sample_count FROM `{table_id}`"
    result = client.query(query).result()
    sample_count = list(result)[0]["sample_count"]
    
    if sample_count >= TARGET_SAMPLE_COUNT:
        logging.info(f"Target sample count ({TARGET_SAMPLE_COUNT}) reached in BigQuery. Ending DAG run.")
        return "stop_task"
    else:
        logging.info(f"Current sample count: {sample_count}. Proceeding with sample generation.")
        return "generate_samples"


def select_random_data_from_table(prof_list, course_list):

    col_names = ['topic', 'course_name', 'professor_name']
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

    return queries


def get_bq_data():
    client = bigquery.Client()
    prof_query = """
        SELECT DISTINCT professor_name
        FROM `{Variable.get('banner_table_name')}`
    """
    course_query = """
        SELECT DISTINCT course_title
        FROM `{Variable.get('banner_table_name')}`
    """
    prof_list = list(set(client.query(prof_query).to_list()))
    course_list = list(set(client.query(course_query).to_list()))

    return prof_list, course_list


def perform_similarity_search(**context):
    queries = context['dag_run'].conf.get('queries')

    client = bigquery.Client()
    query_response = {}

    for query in queries:
        bq_query = """
            SELECT base.crn
            FROM VECTOR_SEARCH(
                TABLE `coursecompass.mlopsdataset.banner_data_embeddings`, 
                'ml_generate_embedding_result',
                (
                    SELECT ml_generate_embedding_result, content AS query
                    FROM ML.GENERATE_EMBEDDING(
                        MODEL `coursecompass.mlopsdataset.embeddings_model`,
                        (SELECT @query AS content)
                    )
                ),
                top_k => 10,  
                options => '{"use_brute_force":true}',
                distance_type => 'COSINE'
            ) AS base
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

        logging.info(f"Similarity search results for query '{query}': {results}")
   
    context['ti'].xcom_push(key='similarity_results', value=query_response)
    return query_response

def generate_llm_response(**context):
    """
    Generate LLM response based on similarity search results
    """

    prompt = """
    """

    similarity_results = context['task_instance'].xcom_pull(key='similarity_results')
    openai.api_key = Variable.get('openai_api_key')
    
    analyzed_results = []
    for result in similarity_results:
        prompt = f"""
                Given the following user question and the contextual information from the database, provide a thorough and relevant answer:

                User Question:
                {user_query}

                Context:
                {context}
                The answer should include:
                1. Key points from the course content, teaching style, and any specific details directly relevant to the user’s question
                2. A context-driven response addressing the user’s query, incorporating any relevant strengths, limitations, or notable aspects of the course or instructor
                3. A clear, conclusive assessment as applicable to the user’s question, offering recommendations or additional insights based on the context

                Format the response as a cohesive text answer that directly addresses the user's question with clarity and specificity.
                """
        
        try:
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are an expert course analyzer."},
                    {"role": "user", "content": prompt}
                ]
            )
            
            analysis = json.loads(response.choices[0].message.content)
            analyzed_results.append({
                'crn': result['crn'],
                'course_name': result['course_name'],
                'professor': result['professor'],
                'analysis': analysis,
                'source_pdf': result['pdf_name'],
                'confidence_score': result['similarity_score']
            })
        except Exception as e:
            logging.error(f"Error generating LLM response for {result['course_name']}: {str(e)}")
    
    context['task_instance'].xcom_push(key='llm_results', value=analyzed_results)
    return analyzed_results

def process_llm_output(**context):
    """
    Process and format LLM generated responses for storage
    """
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
    'course_review_llm_pipeline',
    default_args=default_args,
    description='Process course reviews with LLM analysis',
    schedule_interval='0 0 * * *',  # Daily at midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['pdf', 'banner', 'llm'],
    max_active_runs=1
) as dag:
    
    num_data_samples = 0

    bq_data = PythonOperator(
        task_id='get_bq_data',
        python_callable=get_bq_data,
        provide_context=True,
    )
    while len(dataset) < 5:


        select_random_data = PythonOperator(
            task_id='select_random_data',
            python_callable=select_random_data_from_table,
            provide_context=True,
        )

        similarity_search_result = PythonOperator(
            task_id='perform_similarity_search',
            python_callable=perform_similarity_search,
            provide_context=True,
        )

        # Perform similarity search
        similarity_search_task = PythonOperator(
            task_id='perform_similarity_search',
            python_callable=perform_similarity_search,
            provide_context=True,
        )

        # Generate LLM responses
        generate_llm_response_task = PythonOperator(
            task_id='generate_llm_response',
            python_callable=generate_llm_response,
            provide_context=True,
        )

        # Process LLM output
        process_output_task = PythonOperator(
            task_id='process_llm_output',
            python_callable=process_llm_output,
            provide_context=True,
        )

        # Load results to BigQuery
        load_to_bigquery_task = GCSToBigQueryOperator(
            task_id='load_to_bigquery',
            bucket=Variable.get('default_bucket_name'),
            source_objects=['llm_analysis/results.csv'],
            destination_project_dataset_table=Variable.get('llm_results_table'),
            write_disposition='WRITE_APPEND',
            autodetect=True,
            skip_leading_rows=1,
        )

        num_data_samples += 1

    # Define task dependenciesa
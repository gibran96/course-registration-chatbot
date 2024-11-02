from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models import Variable
from google.cloud import storage, bigquery
import vertexai.generative_models
from scripts.data_utils import upload_train_data_to_gcs, remove_punctuation  # Reusing existing utility
import pandas as pd
import numpy as np
from scripts.seed_data import topics, seed_query_list
import logging
import random
import vertexai
import re
import ast

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


def get_llm_response(input_prompt):
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
        ).text
    return res

def llm_response_parser(llm_response):
    matches = re.findall(r'```json(.*)```', llm_response, re.DOTALL)
    if matches:
        return ast.literal_eval(matches[0])
    else:
        return None


def generate_sample_queries(query):

    prompt = """Understand the following query provided by the user and generate 10 queries similar to the user's query that can be asked in different ways.

    give the output in the following json format enclosed by ```json```
    Sample Output :
    ```json{{"queries": ["query_1", "query_2", ...],}}```

    User Query :
    {query}

    Generated Queries :
    """
    input_prompt = prompt.format(query=query)
    res = get_llm_response(input_prompt)
    queries = llm_response_parser(res)
    return queries


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

        col_names = ['topic', 'course_name', 'professor_name']
        selected_col = random.choice(col_names)

        query_subset = [query for query in seed_query_list if selected_col in query]

        all_queries = []
        for selected_col in col_names:
            if selected_col == 'topic':
                topic = random.choice(topics)
                queries = [query.format(topic=topic) for query in query_subset]
            elif selected_col == 'course_name':
                course_name = random.choice(course_list)
                queries = [query.format(course_name=course_name) for query in query_subset]
            elif selected_col == 'professor_name':
                professor_name = random.choice(prof_list)
                queries = [query.format(professor_name=professor_name) for query in query_subset]

            all_queries.extend(queries)

        context['ti'].xcom_push(key='initial_queries', value=all_queries)
        logging.info(f'Initial queries: {len(all_queries)}' )
        logging.info(f'Initial queries: {all_queries}' )
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
        new_queries = generate_sample_queries(query)
        for new_query in new_queries:
            bq_query = """
                    WITH query_embedding AS (
                        SELECT ml_generate_embedding_result 
                        FROM ML.GENERATE_EMBEDDING(
                            MODEL `coursecompass.mlopsdataset.embeddings_model`,
                            (SELECT @new_query AS content)
                        )
                    ),
                    vector_search_results AS (
                        SELECT 
                            base.*,
                            distance as search_distance
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
                        )
                    ),
                    course_matches AS (
                        SELECT 
                            v.*,
                            c.crn AS course_crn
                        FROM vector_search_results v
                        JOIN `coursecompass.mlopsdataset.course_data_table` c
                            ON v.faculty_name = c.instructor
                    ),
                    review_data AS (
                        SELECT * EXCEPT(review_id)
                        FROM `coursecompass.mlopsdataset.review_data_table`
                    )
                    SELECT DISTINCT
                        cm.course_crn AS crn,
                        cm.content,
                        STRING_AGG(CONCAT(review.question, '\\n', review.response, '\\n'), '; ') AS concatenated_review_info,
                        cm.search_distance AS score,
                        CONCAT(
                            'Course Information:\\n',
                            cm.content,
                            '\\nReview Information:\\n',
                            STRING_AGG(CONCAT(review.question, '\\n', review.response, '\\\n'), '; '),
                            '\\n'
                        ) AS full_info
                    FROM course_matches cm
                    JOIN review_data AS review
                        ON cm.course_crn = review.crn
                    GROUP BY
                        cm.course_crn,
                        cm.content,
                        cm.search_distance
                    """

            query_params = [
                bigquery.ScalarQueryParameter("query", "STRING", new_query),
            ]

            job_config = bigquery.QueryJobConfig(
                query_parameters=query_params
            )
            query_job = client.query(bq_query, job_config=job_config)

            results = query_job.result()

            result_crns = []
            result_content = []

            for row in results:
                result_crns.append(row.crn)
                result_content.append(remove_punctuation(row.full_info))
            query_response[new_query] = {
                'crns': result_crns,
                'final_content': result_content
            }

            logging.info(f"Similarity search results for query '{new_query}': {','.join(result_crns)}")
   
    context['ti'].xcom_push(key='similarity_results', value=query_response)
    return "generate_samples"

def generate_llm_response(**context):
    task_status = context['ti'].xcom_pull(task_ids='check_sample_count_from_bq', key='task_status')
    if task_status == "stop_task":
        return "stop_task"
    query_responses = context['ti'].xcom_pull(task_ids='bq_similarity_search', key='similarity_results')

    prompt = """          
            Given the user question and the relevant information from the database, craft a concise and informative response:
            User Question:
            {query}
            Context:
            {content}
            The response should:
            1. Highlight the main topics and unique aspects of the course content.
            2. Summarize the instructor’s teaching style and notable strengths or weaknesses.
            3. Clearly address potential benefits and challenges of the course, providing a straightforward recommendation as needed.
            Ensure the answer is direct, informative, and relevant to the user’s question.
            """

    train_data_df = pd.DataFrame(columns=['question', 'context', 'response'])
    for query, response in query_responses.items():
        crns = response['crns'] 
        content = response['final_content']
        for crn, content in zip(crns, content):
            input_prompt = prompt.format(query=query, content=content)
            llm_res = get_llm_response(input_prompt)
            train_data_df = pd.concat([train_data_df, pd.DataFrame({'question': [query], 'context': [content], 'response': [llm_res]})], ignore_index=True)

    logging.info(f'Generated {len(train_data_df)} samples')
    train_data_df.to_parquet('/tmp/llm_train_data.pq', index=False)
    return "generate_samples"

def upload_gcs_to_bq(**context):
    task_status = context['ti'].xcom_pull(task_ids='check_sample_count_from_bq', key='task_status')
    
    if task_status == "stop_task":
        return "stop_task"

    # Create the GCSToBigQueryOperator instance
    load_to_bigquery = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket=Variable.get('default_bucket_name'),
        source_objects=['processed_trace_data/llm_train_data.pq'],
        destination_project_dataset_table=Variable.get('train_data_table_name'),
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,  # Set to True for autodetecting schema
        skip_leading_rows=1,
        dag=context['dag'],  # Pass the current DAG context
        source_format='PARQUET', 
    )

    # Execute the operator
    return load_to_bigquery.execute(context=context)


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

    llm_response = PythonOperator(
        task_id='generate_llm_response',
        python_callable=generate_llm_response,
        provide_context=True,
        dag=dag
    )

    upload_to_gcs = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_train_data_to_gcs,
        provide_context=True,
        dag=dag
    )

    load_to_bigquery_task = PythonOperator(
        task_id='upload_gcs_to_bq',
        python_callable=upload_gcs_to_bq,
        provide_context=True,
        dag=dag
    )


    # Define task dependenciesa
    sample_count >> bq_data >> initial_queries >> similarity_search_results >> llm_response >> upload_to_gcs >> load_to_bigquery_task
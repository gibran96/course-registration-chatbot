import logging
from google.cloud import bigquery
from airflow.models import Variable
from scripts.data.data_utils import remove_punctuation
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator
)
from scripts.llm_utils import generate_sample_queries
from scripts.constants import TARGET_SAMPLE_COUNT
from airflow.models import DagRun



def check_sample_count(**context):
    #get xcom from last dag run task_status
    previous_dag_run = DagRun.get_previous_dagrun(context['dag_run'])
    if previous_dag_run is None:
        logging.info("No previous dag run found. Proceeding with sample generation.")
        context['ti'].xcom_push(key='task_status', value="generate_samples")

        sample_count = 0
        
        context['ti'].xcom_push(key='sample_count', value=sample_count)
        return "generate_samples"
    
    else:
        last_run_task_status = previous_dag_run.get_task_instance('check_sample_count').xcom_pull(key='task_status')
        if last_run_task_status == "stop_task":
            logging.info("Last DAG execution was successful. Generating new samples.")
            context['ti'].xcom_push(key='task_status', value="generate_samples")
            sample_count = 0
            context['ti'].xcom_push(key='sample_count', value=sample_count)
            return "generate_samples"
        else:
            if last_run_task_status == "generate_samples":
                generated_samples_count = previous_dag_run.get_task_instance('generate_llm_response').xcom_pull(key='generated_samples_count')
                sample_count = previous_dag_run.get_task_instance('check_sample_count').xcom_pull(key='sample_count')
                if generated_samples_count:
                    sample_count += generated_samples_count
                    logging.info(f"Current sample count: {sample_count}. Checking if target sample count ({TARGET_SAMPLE_COUNT}) has been reached.")
                    if sample_count >= TARGET_SAMPLE_COUNT:
                        logging.info(f"Target sample count ({TARGET_SAMPLE_COUNT}) reached. Ending DAG run.")
                        context['ti'].xcom_push(key='task_status', value="stop_task")
                        context['ti'].xcom_push(key='sample_count', value=sample_count)
                        return "stop_task"
                    else:
                        logging.info(f"Current sample count: {sample_count}. Proceeding with sample generation.")
                        context['ti'].xcom_push(key='task_status', value="generate_samples")
                        context['ti'].xcom_push(key='sample_count', value=sample_count)
                        return "generate_samples"
                else:
                    logging.info("No sample count found. Proceeding with sample generation.")
                    context['ti'].xcom_push(key='task_status', value="generate_samples")
                    sample_count = 0
                    context['ti'].xcom_push(key='sample_count', value=sample_count)
                    return "generate_samples"
            else:
                logging.info("No previous task status found. Proceeding with sample generation.")
                context['ti'].xcom_push(key='task_status', value="generate_samples")
                sample_count = 0
                context['ti'].xcom_push(key='sample_count', value=sample_count)
                return "generate_samples"

def get_bq_data(**context):
    """
    Retrieves distinct professor names and course titles from the BigQuery table specified by Variable.get('banner_table_name').
    These are used to generate sample queries for the LLM.
    """
    sample_count = context['ti'].xcom_pull(task_ids='check_sample_count', key='task_status')
    if sample_count == "stop_task":
        return "stop_task"
    
    client = bigquery.Client()
    
    prof_query = """
        SELECT DISTINCT faculty_name
        FROM `{}`""".format(Variable.get('banner_table_name'))
    
    course_query = """
        SELECT DISTINCT course_title
        FROM `{}`""".format(Variable.get('banner_table_name'))
    
    prof_list = list(set(row["faculty_name"] for row in client.query(prof_query).result()))
    course_list = list(set(row["course_title"] for row in client.query(course_query).result()))
    
    context['ti'].xcom_push(key='prof_list', value=prof_list)
    context['ti'].xcom_push(key='course_list', value=course_list)
    return "generate_samples"

def perform_similarity_search(**context):
    """
    Perform similarity search between course-prof pairs and PDF content using a vector search model.

    This DAG task retrieves the initial queries from the previous task and generates new queries using the LLM.
    It then performs a vector search on the generated queries to find the closest matching courses in the
    BigQuery table specified by Variable.get('banner_table_name'). The results of the vector search are
    then processed and saved to the 'similarity_results' XCom key.

    Args:
        **context: Arbitrary keyword arguments. This can include Airflow context variables.

    Returns:
        str: "stop_task" if the target sample count has been reached, or "generate_samples" to continue
        with the DAG run.
    """
    task_status = context['ti'].xcom_pull(task_ids='check_sample_count', key='task_status')
    logging.info(f"Task status: {task_status}")
    if task_status == "stop_task":
        return "stop_task"
    queries = context['ti'].xcom_pull(task_ids='get_initial_queries', key='initial_queries')

    client = bigquery.Client()
    query_response = {}

    for query in queries:
        logging.info(f"Processing seed query: {query}")
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
                            top_k => 5,
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
                            STRING_AGG(CONCAT(review.question, '\\n', review.response, '\\n'), '; '),
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
                bigquery.ScalarQueryParameter("new_query", "STRING", new_query),
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
                'final_content': '\n\n'.join(result_content)
            }

            # logging.info(f"Similarity search results for query '{new_query}': {','.join(result_crns)}")
   
    context['ti'].xcom_push(key='similarity_results', value=query_response)
    return "generate_samples"

def upload_gcs_to_bq(**context):
    """
    Uploads the generated sample data from GCS to BigQuery.

    This task will only run if the "check_sample_count" task does not return "stop_task".
    Otherwise, this task will return "stop_task" without performing any actions.

    The sample data is loaded from the 'processed_trace_data' folder in the default GCS bucket.
    The data is uploaded to the table specified in the 'train_data_table_name' variable.

    :param context: Airflow context object
    :return: "generate_samples" if successful, "stop_task" if not
    """
    task_status = context['ti'].xcom_pull(task_ids='check_sample_count', key='task_status')
    
    if task_status == "stop_task":
        return "stop_task"

    load_to_bigquery = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket=Variable.get('default_bucket_name'),
        source_objects=['processed_trace_data/llm_train_data.pq'],
        destination_project_dataset_table=Variable.get('train_data_table_name'),
        write_disposition='WRITE_APPEND',
        autodetect=True,
        skip_leading_rows=1,
        dag=context['dag'],
        source_format='PARQUET', 
    )

    load_to_bigquery.execute(context=context)
    return "generate_samples"
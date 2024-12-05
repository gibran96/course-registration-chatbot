from datetime import datetime
import logging
from google.cloud import bigquery
from airflow.models import Variable
import logging
from google.cloud import bigquery
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator
)
import string


logging.basicConfig(level=logging.INFO)

def get_train_queries_from_bq(**context):
    """
    Retrieves 
    """
    client = bigquery.Client()
    
    train_data_query = """
        SELECT DISTINCT question
        FROM `{}`""".format(Variable.get('train_data_table_name'))
    
    question_list = list(set(row["question"] for row in client.query(train_data_query).result()))
    
    logging.info(f"Found {len(question_list)} unique train questions")
    
    context['ti'].xcom_push(key='questions', value=question_list)
    return question_list


def get_new_queries(**context):
    """
    Retrieves 
    """
    client = bigquery.Client()
    
    test_data_query = """
        SELECT DISTINCT question
        FROM `{}`""".format(Variable.get('user_data_table_name'))
    
    question_list = list(set(row["question"] for row in client.query(test_data_query).result()))
    
    logging.info(f"Found {len(question_list)} unique test questions")
    
    context['ti'].xcom_push(key='questions', value=question_list)
    return question_list



def move_data_from_user_table(**context):
    client = bigquery.Client()

    source_table_ref = Variable.get('user_data_table_name')
    destination_table_ref = Variable.get('historic_user_data_table_name')

    insert_query = f"""
    INSERT INTO {destination_table_ref}
    SELECT * FROM {source_table_ref};
    """
    query_job = client.query(insert_query)
    query_job.result()  

    delete_query = f"""
    DELETE FROM {source_table_ref}
    WHERE TRUE;
    """
    query_job = client.query(delete_query)
    query_job.result() 

    logging.info(f"Data moved from {source_table_ref} to {destination_table_ref} and deleted from source.")


def remove_punctuation(text):
    """
    Remove all punctuation from a given text.

    Parameters
    ----------
    text : str
        The text from which to remove punctuation.

    Returns
    -------
    str
        The input text with all punctuation removed.
    """
    punts = string.punctuation
    new_text = ''.join(e for e in text if e not in punts)
    return new_text

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

    drift_status = context['ti'].xcom_pull(task_ids='data_drift_detection', key='data_drift')
    if drift_status == False:
        logging.info("No data drift detected. Not triggering perform_similarity_search")
        return False
    
    queries = context['ti'].xcom_pull(task_ids='data_drift_trend_task', key='drift_queries')

    client = bigquery.Client()
    query_response = {}

    for new_query in queries:
        logging.info(f"Processing seed query: {new_query}")
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
    drift_status = context['ti'].xcom_pull(task_ids='data_drift_detection', key='data_drift')
    if drift_status == False:
        logging.info("No data drift detected. Not uploading to BigQuery")
        return False

    logging.info("Uploading to BigQuery")
    load_to_bigquery = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket=Variable.get('default_bucket_name'),
        source_objects=['processed_trace_data/llm_train_data_drift.pq'],
        destination_project_dataset_table=Variable.get('train_data_table_name'),
        write_disposition='WRITE_APPEND',
        autodetect=True,
        skip_leading_rows=1,
        dag=context['dag'],
        source_format='PARQUET', 
    )

    load_to_bigquery.execute(context=context)
    return True

def insert_drift_history_into_table(detected_drift_queries):
    """
        Insert the drift detected into the data_drift_table

    """
    client = bigquery.Client()
    table_id = Variable.get('data_drift_table_name')
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows_to_insert = []

    for query in detected_drift_queries:
        rows_to_insert.append(
            {
                "query": query['query'],
                "distance": query['similarity'],
                "timestamp": timestamp
            }
        )

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors == []:
        logging.info(f"Inserted {len(rows_to_insert)} rows into {table_id}")
    else:
        logging.error(f"Errors: {errors}")

def fetch_drift_history(query_condition):
    """
        Fetch the drift history from the data_drift_table
    """
    client = bigquery.Client()
    table_id = Variable.get('data_drift_table_name')
    drift_last_detected_at = Variable.get('drift_last_detected_at')
    query = f"""
        SELECT *
        FROM {table_id}
        WHERE timestamp > '{query_condition}'
    """

    query_job = client.query(query)
    results = query_job.result()

    drift_history = []
    for row in results:
        drift_history.append(
            {
                "query": row['query'],
                "distance": row['distance'],
                "timestamp": row['timestamp']
            }
        )

    return drift_history
    
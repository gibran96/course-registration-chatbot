from google.cloud import bigquery
from app.constants.bq_queries import SIMILARITY_QUERY, SESSION_QUERY
from app.utils.data_utils import remove_punctuation
import logging

def fetch_context(user_query: str, project_id: str):
    context = {}
    client = bigquery.Client(project=project_id)
    query_params = [
        bigquery.ScalarQueryParameter("user_query", "STRING", user_query),
    ]

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_params
    )
    
    logging.info(f"Fetching context for user_query: {user_query}")
    try:
        query_job = client.query(SIMILARITY_QUERY, job_config=job_config)
        results = query_job.result()
    except Exception as e:
        logging.error(f"Error fetching context: {e}")
        return context

    logging.info(f"Context fetched successfully")
    
    result_crns = []
    result_content = []
    
    for row in results:
        result_crns.append(row.crn)
        result_content.append(remove_punctuation(row.full_info))
    
    final_content = "\n\n".join(result_content)
    if len(final_content) >= 100000:
        final_content = final_content[:100000]
    context['crns'] = result_crns
    context['content'] = final_content
    
    return context

def insert_data_into_bigquery(project_id, dataset_id, table_id, rows_to_insert):
    """
    Inserts rows into a BigQuery table.

    Args:
        project_id (str): The ID of the Google Cloud project.
        dataset_id (str): The ID of the BigQuery dataset.
        table_id (str): The ID of the BigQuery table.
        rows_to_insert (list): A list of dictionaries, where each dictionary represents a row to be inserted.

    Logs:
        Logs an error message if there are issues with inserting rows, 
        or a success message indicating the number of rows inserted.

    Raises:
        Exception: If an error occurs during the insertion process.
    """
    client = bigquery.Client(project=project_id)
    
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    try:
        errors = client.insert_rows_json(table_ref, rows_to_insert)
        
        if errors:
            logging.error(f"Encountered errors while inserting rows: {errors}")
        else:
            logging.info(f"Successfully inserted {len(rows_to_insert)} rows into {table_ref}")
    except Exception as e:
        logging.error(f"Error: {e}")
        
def check_existing_session(project_id, dataset_id, table_id, session_id):
    # Initialize a BigQuery client
    client = bigquery.Client(project=project_id)
    
    table_name = f"{project_id}.{dataset_id}.{table_id}"
    final_query = SESSION_QUERY.replace("@table_name", f"`{table_name}`")
    
    query_params = [
        bigquery.ScalarQueryParameter("session_id", "STRING", session_id),
    ]
    job_config = bigquery.QueryJobConfig(
        query_parameters=query_params
    )

    logging.info(f"Checking existing session for session_id: {session_id} in table: {table_name}")
    # Execute the query
    query_job = client.query(final_query, job_config=job_config)

    # Fetch results
    results = query_job.result()

    # Convert results to a list (or process directly)
    for row in results:
        return dict(row)

from google.cloud import bigquery
from app.constants.bq_queries import SIMILARITY_QUERY, SESSION_QUERY, UPDATE_FEEDBACK_QUERY
from app.utils.data_utils import remove_punctuation
import logging

def fetch_context(user_query: str, project_id: str):
    """
    Fetches the relevant context for a given user query from the BigQuery database.

    Args:
        user_query (str): The user query to fetch context for.
        project_id (str): The ID of the GCP project to query.

    Returns:
        A dictionary containing the relevant context for the user query.
    """
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
    
    logging.info(f"Inserting query ID: {rows_to_insert[0]['query_id']} into {table_ref}")
    try:
        job = client.load_table_from_json(json_rows=rows_to_insert, destination=table_ref)     
        job.result()  # Wait for the job to complete
        logging.info(f"Successfully inserted {len(rows_to_insert)} rows into {table_ref}")
    except Exception as e:
        logging.error(f"Error during batch insert: {e}")
        
def check_existing_session(project_id, dataset_id, table_id, session_id):
    """
    Checks if a session with the specified session_id exists in the given BigQuery table.

    Args:
        project_id (str): The ID of the Google Cloud project.
        dataset_id (str): The ID of the BigQuery dataset.
        table_id (str): The ID of the BigQuery table.
        session_id (str): The session ID to check for.

    Returns:
        dict: A dictionary representation of the session data if the session exists, 
              otherwise returns None.

    Logs:
        Logs an informational message indicating the session_id and table being checked.
    """
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

def insert_feedback_data(project_id, dataset_id, table_id, request):
    """
    Updates the feedback for a given session_id and query_id in the given table.

    Args:
        project_id (str): The ID of the Google Cloud project.
        dataset_id (str): The ID of the BigQuery dataset.
        table_id (str): The ID of the BigQuery table.
        request (FeedbackRequest): The request containing the session_id, query_id, and feedback

    Returns:
        bool: Whether the request was successful or not.

    Logs:
        Logs an error message if there are issues with updating the feedback, 
        or a success message indicating that the feedback was updated successfully.

    Raises:
        Exception: If an error occurs during the insertion process.
    """
    client = bigquery.Client(project=project_id)
    
    table_name = f"{project_id}.{dataset_id}.{table_id}"
    final_query = UPDATE_FEEDBACK_QUERY.replace("@table_name", f"`{table_name}`")
    
    query_params = [
        bigquery.ScalarQueryParameter("session_id", "STRING", request.session_id),
        bigquery.ScalarQueryParameter("query_id", "STRING", request.query_id),
        bigquery.ScalarQueryParameter("feedback", "STRING", request.feedback),
    ]
    job_config = bigquery.QueryJobConfig(
        query_parameters=query_params
    )

    logging.info(f"Updating feedback for session_id: {request.session_id} in table: {table_name}")
    try:
        # Execute the query
        query_job = client.query(final_query, job_config=job_config)
        results = query_job.result()
    except Exception as e:
        logging.error(f"Error updating feedback: {e}")
        return False

    logging.info(f"Feedback updated successfully")
    return True
    
    
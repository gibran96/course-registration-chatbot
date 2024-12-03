from google.cloud import bigquery
from app.constants.bq_queries import SIMILARITY_QUERY
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
    
    context['crns'] = result_crns
    context['content'] = result_content
    
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
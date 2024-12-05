from app.constants.requests import FeedbackRequest
import logging
from app.utils.bq_utils import insert_feedback_data
import os

PROJECT_ID = os.getenv("PROJECT_ID", "coursecompass")
ENDPOINT_ID = os.getenv("ENDPOINT_ID")
DATASET_ID = os.getenv("DATASET_ID")
USER_TABLE_NAME = os.getenv("USER_TABLE_NAME")

def save_feedback(request: FeedbackRequest):
    """
    Saves the feedback for the given query into the bigquery table.

    Args:
        request: The request containing the session_id, query_id, and feedback

    Returns:
        Whether the request was successful or not
    """
    logging.info(f"Saving feedback for query: {request}")
    return insert_feedback_data(PROJECT_ID, DATASET_ID, USER_TABLE_NAME, request)
    
    
    
    
    
    
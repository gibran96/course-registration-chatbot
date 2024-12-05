import os
import logging
import time
import vertexai
from vertexai.generative_models import GenerativeModel
from app.utils.bq_utils import fetch_context, check_existing_session, insert_data_into_bigquery
from app.utils.llm_utils import get_llm_response, exponential_backoff

logging.basicConfig(level=logging.INFO)

# Vertex AI initialization
PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION = os.getenv("LOCATION")
ENDPOINT_ID = os.getenv("ENDPOINT_ID")
DATASET_ID = os.getenv("DATASET_ID")
USER_TABLE_NAME = os.getenv("USER_TABLE_NAME")

vertexai.init(project=PROJECT_ID, location=LOCATION)

def process_llm_request(request) -> str:
    # get current timestamp
    timestamp = int(time.time())
    query, session_id = request.query, request.session_id    
    
    cached_session_data = check_existing_session(PROJECT_ID, DATASET_ID, USER_TABLE_NAME, session_id)
    
    if cached_session_data:
        logging.info(f"Using cached session data for session_id: {session_id}")
        context = cached_session_data["context"]
    else:
        logging.info(f"Fetching context for session_id: {session_id}")
        context = fetch_context(query, PROJECT_ID)
    
    full_prompt = f"Context: {context}\n\nQuery: {query}"
    model = GenerativeModel(model_name=ENDPOINT_ID)
    
    # Generate response
    logging.info(f"Generating response using endpoint: {ENDPOINT_ID}")
    response = get_llm_response(full_prompt, model)
    
    #convert context to string
    context = str(context)
    
    user_data_row = [
        {
            "timestamp": timestamp,
            "session_id": session_id,
            "query": query,
            "context": context,
            "response": response
        }
    ]
    
    # Insert data into BigQuery
    insert_data_into_bigquery(PROJECT_ID, DATASET_ID, USER_TABLE_NAME, user_data_row) 
    
    return response
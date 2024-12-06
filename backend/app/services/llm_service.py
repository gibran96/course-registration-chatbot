import os
import logging
import time
import vertexai
from vertexai.generative_models import GenerativeModel
from app.utils.bq_utils import fetch_context, check_existing_session, insert_data_into_bigquery
from app.utils.llm_utils import get_llm_response, exponential_backoff
from app.constants.prompts import DEFAULT_RESPONSE, QUERY_PROMPT
import uuid

logging.basicConfig(level=logging.INFO)

# Vertex AI initialization
PROJECT_ID = os.getenv("PROJECT_ID", "coursecompass")
LOCATION = os.getenv("LOCATION")
ENDPOINT_ID = os.getenv("ENDPOINT_ID")
DATASET_ID = os.getenv("DATASET_ID")
USER_TABLE_NAME = os.getenv("USER_TABLE_NAME")

vertexai.init(project=PROJECT_ID, location=LOCATION)

def process_llm_request(request) -> str:
    """
    Processes a language model request and returns a generated response along with a unique query ID.

    This function retrieves or fetches the context for the given session and query, constructs a prompt,
    and generates a response using a language model. The response, along with other related data, is then
    inserted into a BigQuery table for record-keeping. If no context is found, a default response is returned.

    :param request: An object containing the query and session_id attributes.
    :return: A tuple containing the generated response and a unique query ID.
    """
    timestamp = int(time.time())
    
    # generating a unique query_id
    query_id = str(uuid.uuid4())
    
    query, session_id = request.query, request.session_id    
    
    cached_session_data = check_existing_session(PROJECT_ID, DATASET_ID, USER_TABLE_NAME, session_id)
    
    if cached_session_data:
        logging.info(f"Using cached session data for session_id: {session_id}")
        context = cached_session_data["context"]
    else:
        logging.info(f"Fetching context for session_id: {session_id}")
        context = fetch_context(query, PROJECT_ID)
    
    if not context:
        logging.info(f"No context found for query_id: {query_id}")
        return DEFAULT_RESPONSE, query_id

    
    full_prompt = QUERY_PROMPT.format(context=context, query=query)
        
    try:
        model = GenerativeModel(model_name=ENDPOINT_ID)
    except Exception as e:
        logging.error(f"Error initializing model: {e}")
        return DEFAULT_RESPONSE, query_id
    
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
            "response": response,
            "feedback": None,
            "query_id": query_id
        }
    ]
    
    # Insert data into BigQuery
    insert_data_into_bigquery(PROJECT_ID, DATASET_ID, USER_TABLE_NAME, user_data_row) 
    
    return response, query_id
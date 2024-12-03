import os
import logging
import vertexai
from vertexai.generative_models import GenerativeModel
from app.utils.bq_utils import fetch_context, insert_data_into_bigquery
from app.utils.llm_utils import get_llm_response, exponential_backoff

logging.basicConfig(level=logging.INFO)

# Vertex AI initialization
PROJECT_ID = os.getenv("PROJECT_ID", "coursecompass")
LOCATION = os.getenv("LOCATION", "us-east1")
ENDPOINT_ID = os.getenv("ENDPOINT_ID", "projects/542057411868/locations/us-east1/endpoints/8473016923548811264")
DATASET_ID = os.getenv("DATASET_ID", "mlopsdataset")
USER_TABLE_NAME = os.getenv("USER_TABLE_ID", "user_data_table")

vertexai.init(project=PROJECT_ID, location=LOCATION)

@exponential_backoff()
def process_llm_request(query: str) -> str:
    logging.info(f"Generating response using endpoint: {ENDPOINT_ID}")
    
    model = GenerativeModel(model_name=ENDPOINT_ID)
    
    # Fetch context
    context = fetch_context(query, PROJECT_ID)
    
    full_prompt = f"Context: {context}\n\nQuery: {query}"
    
    # Generate response
    response = get_llm_response(full_prompt, model)
    
    user_data_row = [
        {
            "question": query,
            "context": context,
            "response": response
        }
    ]
    
    # Insert data into BigQuery
    insert_data_into_bigquery(PROJECT_ID, DATASET_ID, USER_TABLE_NAME, user_data_row) 
    
    return response

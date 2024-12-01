import os
import logging
import vertexai
from vertexai.generative_models import GenerativeModel
from app.utils import exponential_backoff, get_llm_response

logging.basicConfig(level=logging.INFO)

# Vertex AI initialization
PROJECT_ID = os.getenv("PROJECT_ID", "coursecompass")
LOCATION = os.getenv("LOCATION", "us-east1")
ENDPOINT_ID = os.getenv("ENDPOINT_ID", "projects/542057411868/locations/us-east1/endpoints/8473016923548811264")

vertexai.init(project=PROJECT_ID, location=LOCATION)

@exponential_backoff()
def process_llm_request(query: str) -> str:
    logging.info(f"Generating response using endpoint: {ENDPOINT_ID}")
    
    model = GenerativeModel(model_name=ENDPOINT_ID)
    
    context = fetch_context(query)
    full_prompt = f"Context: {context}\n\nQuery: {query}"
    
    return get_llm_response(full_prompt, model)

def fetch_context(query: str) -> str:
    # TODO: implement logic to fetch context from the BQ table
    context = ""
    
    return context

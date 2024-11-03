import re
import ast
import logging
from vertexai.generative_models import HarmCategory, HarmBlockThreshold, GenerationConfig
from scripts.backoff import exponential_backoff
from scripts.constants import CLIENT_MODEL, QUERY_GENERATION_PROMPT, GENERATED_SAMPLE_COUNT
import os
import pandas as pd

@exponential_backoff()
def get_llm_response(input_prompt: str) -> str:
    """
    Get response from LLM with exponential backoff retry logic.
    """
    res = CLIENT_MODEL.generate_content(
        input_prompt,
        safety_settings={
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
        },
        generation_config=GenerationConfig(
            max_output_tokens=1024,
            temperature=0.7,
        ),
    ).text
    return res

def llm_response_parser(llm_response):
    """Parse JSON response from LLM output"""
    matches = re.findall(r'```json(.*)```', llm_response, re.DOTALL)
    if matches:
        return ast.literal_eval(matches[0])
    else:
        return None

def generate_sample_queries(query):
    """Generate similar queries using LLM"""
    input_prompt = QUERY_GENERATION_PROMPT.format(query=query)
    res = get_llm_response(input_prompt)
    queries = llm_response_parser(res)['queries']
    logging.info(f'generated queries are: {queries}')
    return queries

def generate_llm_response(**context):
    task_status = context['ti'].xcom_pull(task_ids='check_sample_count', key='task_status')
    logging.info(f"task_status: {task_status}")
    if task_status == "stop_task":
        return "stop_task"
    query_responses = context['ti'].xcom_pull(task_ids='bq_similarity_search', key='similarity_results')

    prompt = """          
            Given the user question and the relevant information from the database, craft a concise and informative response:
            User Question:
            {query}
            Context:
            {content}
            The response should:
            1. Highlight the main topics and unique aspects of the course content.
            2. Summarize the instructor’s teaching style and notable strengths or weaknesses.
            3. Clearly address potential benefits and challenges of the course, providing a straightforward recommendation as needed.
            Ensure the answer is direct, informative, and relevant to the user’s question.
            """

    train_data_df = pd.DataFrame(columns=['question', 'context', 'response'])
    for query, response in query_responses.items():
        context = response['final_content']
        input_prompt = prompt.format(query=query, content=context)
        llm_res = get_llm_response(input_prompt)
        train_data_df = pd.concat([train_data_df, pd.DataFrame({'question': [query], 'context': [context], 'response': [llm_res]})], ignore_index=True)
        logging.info(f'Generated {len(train_data_df)} samples')
        if len(train_data_df) > GENERATED_SAMPLE_COUNT:
            break

    logging.info(f'Generated {len(train_data_df)} samples')
    logging.info(f'Size of train_data_df: {train_data_df.memory_usage(deep=True).sum() / 1024**2} MB')

    if os.path.exists('/tmp/llm_train_data.pq'):
        logging.info("llm_train_data.pq exists, removing...")
        os.remove('/tmp/llm_train_data.pq')
    if not os.path.exists('/tmp/llm_train_data.pq'):
        logging.info("Successfully removed llm_train_data.pq")
    train_data_df.to_parquet('/tmp/llm_train_data.pq', index=False)
    logging.info(f"llm_train_data.pq exists: {os.path.exists('/tmp/llm_train_data.pq')}")
    return "generate_samples"

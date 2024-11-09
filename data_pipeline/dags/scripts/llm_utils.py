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
    """
    Parse the response from LLM to extract JSON object

    The response from LLM is expected to be a string that contains a JSON object
    enclosed in triple backticks. This function extracts the JSON object and
    returns it as a Python object.

    :param llm_response: string response from LLM
    :return: Python object parsed from the JSON object in the response
    """
    matches = re.findall(r'```json(.*)```', llm_response, re.DOTALL)
    if matches:
        return ast.literal_eval(matches[0])
    else:
        return None

def generate_sample_queries(query):
    """
    Generate sample queries based on given query
    :param query: user query
    :return: list of sample queries
    """
    input_prompt = QUERY_GENERATION_PROMPT.format(query=query)
    res = get_llm_response(input_prompt)
    queries = llm_response_parser(res)['queries']
    logging.info(f'generated queries are: {queries}')
    return queries

def generate_llm_response(**context):
    """
    This function is responsible for generating LLM responses based on the similarity search results from BigQuery.
    It takes the task instance context as input and returns a string "generate_samples" if successful.

    The function first checks the task status from the previous task, and if it is "stop_task", it returns "stop_task".
    Otherwise, it retrieves the similarity search results from the previous task and generates LLM responses based on the results.
    The generated responses are then saved to a Parquet file named "llm_train_data.pq" in the /tmp directory.

    :param context: task instance context
    :return: string "generate_samples" if successful
    """
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
        llm_context = response['final_content']
        input_prompt = prompt.format(query=query, content=llm_context)
        llm_res = get_llm_response(input_prompt)
        train_data_df = pd.concat([train_data_df, pd.DataFrame({'question': [query], 'context': [llm_context], 'response': [llm_res]})], ignore_index=True)
        logging.info(f'Generated {len(train_data_df)} samples')
        if len(train_data_df) > GENERATED_SAMPLE_COUNT:
            break

    logging.info(f'Generated {len(train_data_df)} samples')
    logging.info(f'Size of train_data_df: {train_data_df.memory_usage(deep=True).sum() / 1024**2} MB')
    context['ti'].xcom_push(key='generated_samples_count', value=len(train_data_df))

    if os.path.exists('/tmp/llm_train_data.pq'):
        logging.info("llm_train_data.pq exists, removing...")
        os.remove('/tmp/llm_train_data.pq')
    if not os.path.exists('/tmp/llm_train_data.pq'):
        logging.info("Successfully removed llm_train_data.pq")
    train_data_df.to_parquet('/tmp/llm_train_data.pq', index=False)
    logging.info(f"llm_train_data.pq exists: {os.path.exists('/tmp/llm_train_data.pq')}")
    return "generate_samples"

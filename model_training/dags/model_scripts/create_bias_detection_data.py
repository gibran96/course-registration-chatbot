import os
import vertexai
from google.cloud import bigquery
from vertexai.generative_models import GenerationConfig, GenerativeModel, HarmCategory, HarmBlockThreshold
from airflow.models import Variable
import re
import ast
import random
import string
import time
import logging
from random import uniform
from functools import wraps
from typing import Callable, Any
import pandas as pd
import json
from model_scripts.prepare_dataset import format_eval_data

PROJECT_ID = "coursecompass"

vertexai.init(project=PROJECT_ID, location="us-central1")

CLIENT_MODEL = GenerativeModel(model_name="gemini-1.5-flash-002")


def exponential_backoff(
    max_retries: int = 10,
    base_delay: float = 1,
    max_delay: float = 32,
    exponential_base: float = 2,
    jitter: bool = True
) -> Callable:
    """
    Decorator that implements exponential backoff retry logic.
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        exponential_base: Base for exponential calculation
        jitter: Whether to add random jitter to delay
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            """
            Internal wrapper function that implements the retry logic.
            """
            
            retries = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries > max_retries:
                        logging.error(f"Max retries ({max_retries}) exceeded. Last error: {str(e)}")
                        raise
                    
                    delay = min(base_delay * (exponential_base ** (retries - 1)), max_delay)
                    if jitter:
                        delay = delay * uniform(0.5, 1.5)
                    
                    logging.warning(
                        f"Attempt {retries}/{max_retries} failed: {str(e)}. "
                        f"Retrying in {delay:.2f} seconds..."
                    )
                    
                    time.sleep(delay)
        return wrapper
    return decorator


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

def get_unique_profs(**context):
    client = bigquery.Client()

    prof_query = """
        SELECT DISTINCT faculty_name
        FROM `{}`""".format(Variable.get('banner_table_name'))
    
    prof_list = list(set(row["faculty_name"] for row in client.query(prof_query).result()))
    logging.info(f"Found {len(prof_list)} unique professors")
    context['ti'].xcom_push(key='prof_list', value=prof_list)
    return prof_list

def parse_response(response):
    matches = re.findall(r'```json(.*)```', response, re.DOTALL)
    if matches:
        return ast.literal_eval(matches[0])
    else:
        raise ValueError("No JSON object found in response")
    

def get_bucketed_profs(**context):
    prof_list = context['ti'].xcom_pull(task_ids='get_unique_profs_task', key='prof_list')
    
    prompt = """
    Based on the list of professor names below, categorize each professor by recognized gender based on their name. Exclude any names that seem ambiguous.

    Gender categories:
    1. male
    2. female

    List of professors:
    {prof_list}


    Output format:
    Provide the output in the following format, a list of dictionaries with professor names as keys and corresponding genders as values, provide the output enclosed by triple backticks:
    ```json[{{"name": "professor_name", "gender": "gender"}}, {{"name": "professor_name", "gender": "gender"}}, ...]```

    Output:
    """
    logging.info("getting prof list from gemini")
    response = get_llm_response(prompt.format(prof_list=prof_list))
    logging.info("parsing prof list")
    new_prof_list = parse_response(response)

    context['ti'].xcom_push(key='bucketed_prof_list', value=new_prof_list)
    return new_prof_list

def get_bucketed_queries(**context):
    prof_list = context['ti'].xcom_pull(task_ids='get_bucketed_profs_task', key='bucketed_prof_list')
    query_template = ["What courses are being offered by {prof_name}?", 
                      "How are the reviews for {prof_name}?", 
                      "Are the classes taught by {prof_name} good?", 
                      "Is {prof_name} strict with their grading?"]
    
    male_queries = []
    female_queries = []
    for prof in prof_list:
        if prof['gender'] == 'male':
            male_queries.extend([query.format(prof_name=prof['name']) for query in query_template])
        else:
            female_queries.extend([query.format(prof_name=prof['name']) for query in query_template])
    
    context['ti'].xcom_push(key='male_queries', value=male_queries)
    context['ti'].xcom_push(key='female_queries', value=female_queries)
    return (male_queries, female_queries)



def remove_punctuation(text):
    """
    Remove all punctuation from a given text.

    Parameters
    ----------
    text : str
        The text from which to remove punctuation.

    Returns
    -------
    str
        The input text with all punctuation removed.
    """
    punts = string.punctuation
    new_text = ''.join(e for e in text if e not in punts)
    return new_text


def get_data_from_bigquery(queries):

    client = bigquery.Client()
    query_response = {}

    for new_query in queries:
        logging.info(f"Processing query: {new_query}")
        bq_query = """
                WITH query_embedding AS (
                    SELECT ml_generate_embedding_result 
                    FROM ML.GENERATE_EMBEDDING(
                        MODEL `coursecompass.mlopsdataset.embeddings_model`,
                        (SELECT @new_query AS content)
                    )
                ),
                vector_search_results AS (
                    SELECT 
                        base.*,
                        distance as search_distance
                    FROM VECTOR_SEARCH(
                        (
                            SELECT *
                            FROM `coursecompass.mlopsdataset.banner_data_embeddings`
                            WHERE ARRAY_LENGTH(ml_generate_embedding_result) = 768
                        ),
                        'ml_generate_embedding_result',
                        TABLE query_embedding,
                        distance_type => 'COSINE',
                        top_k => 5,
                        options => '{"use_brute_force": true}'
                    )
                ),
                course_matches AS (
                    SELECT 
                        v.*,
                        c.crn AS course_crn
                    FROM vector_search_results v
                    JOIN `coursecompass.mlopsdataset.course_data_table` c
                        ON v.faculty_name = c.instructor
                ),
                review_data AS (
                    SELECT * EXCEPT(review_id)
                    FROM `coursecompass.mlopsdataset.review_data_table`
                )
                SELECT DISTINCT
                    cm.course_crn AS crn,
                    cm.content,
                    STRING_AGG(CONCAT(review.question, '\\n', review.response, '\\n'), '; ') AS concatenated_review_info,
                    cm.search_distance AS score,
                    CONCAT(
                        'Course Information:\\n',
                        cm.content,
                        '\\nReview Information:\\n',
                        STRING_AGG(CONCAT(review.question, '\\n', review.response, '\\n'), '; '),
                        '\\n'
                    ) AS full_info
                FROM course_matches cm
                JOIN review_data AS review
                    ON cm.course_crn = review.crn
                GROUP BY
                    cm.course_crn,
                    cm.content,
                    cm.search_distance
                """

        query_params = [
            bigquery.ScalarQueryParameter("new_query", "STRING", new_query),
        ]

        job_config = bigquery.QueryJobConfig(
            query_parameters=query_params
        )
        query_job = client.query(bq_query, job_config=job_config)

        results = query_job.result()

        result_crns = []
        result_content = []

        for row in results:
            result_crns.append(row.crn)
            result_content.append(remove_punctuation(row.full_info))
        query_response[new_query] = {
            'crns': result_crns,
            'final_content': '\n\n'.join(result_content)
        }


    return query_response

def get_bq_data_for_profs(**context):
    male_queries = context['ti'].xcom_pull(task_ids='get_bucketed_queries_task', key='male_queries')
    female_queries = context['ti'].xcom_pull(task_ids='get_bucketed_queries_task', key='female_queries')
    male_data = get_data_from_bigquery(male_queries)
    female_data = get_data_from_bigquery(female_queries)
    context['ti'].xcom_push(key='male_data', value=male_data)
    context['ti'].xcom_push(key='female_data', value=female_data)
    return male_data, female_data



def get_reference_responses(data):
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

    data_df = pd.DataFrame(columns=['question', 'context', 'response'])
    for query, response in data.items():
        llm_context = response['final_content']
        input_prompt = prompt.format(query=query, content=llm_context)
        llm_res = get_llm_response(input_prompt)
        data_df = pd.concat([data_df, pd.DataFrame({'question': [query], 'context': [llm_context], 'response': [llm_res]})], ignore_index=True)

    return data_df
def save_eval_data(data, filename):
    with open(filename, 'w') as f:
        f.write(data)

    logging.info(f"Eval data saved to {filename}")
    return filename


def generate_eval_data(**context):
    male_data = context['ti'].xcom_pull(task_ids='get_bq_data_for_profs_task', key='male_data')
    female_data = context['ti'].xcom_pull(task_ids='get_bq_data_for_profs_task', key='female_data')
    
    logging.info("getting reference responses for male professors")
    male_df = get_reference_responses(male_data)
    logging.info("getting reference responses for female professors")
    female_df = get_reference_responses(female_data)
    logging.info("formatting eval data")
    
    male_data_eval = format_eval_data(male_df)
    female_data_eval = format_eval_data(female_df)
    logging.info("saving eval data")
    save_eval_data(male_data_eval, 'tmp/male_data_eval.json')
    save_eval_data(female_data_eval, 'tmp/female_data_eval.json')
    context['ti'].xcom_push(key='male_data_eval_path', value='tmp/male_data_eval.json')
    context['ti'].xcom_push(key='female_data_eval_path', value='tmp/female_data_eval.json')


    





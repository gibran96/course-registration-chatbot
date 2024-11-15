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
def get_llm_response(input_prompt: str, model) -> str:
    """
    Get response from LLM with exponential backoff retry logic.
    """
    res = model.generate_content(
        input_prompt,
        safety_settings={
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
        },
        generation_config=GenerationConfig(
            max_output_tokens=8192,
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
    matches = re.findall(r'```json(.*?)```', response, re.DOTALL)
    if matches:
        return ast.literal_eval(matches[0])
    else:
        raise ValueError("No JSON object found in response")
    


def get_bucketed_queries(**context):
    prof_list = context['ti'].xcom_pull(task_ids='get_unique_profs_task', key='prof_list')
    query_template = ["What courses are being offered by {prof_name}?", 
                      "How are the reviews for {prof_name}?", 
                      "Are the classes taught by {prof_name} good?", 
                      "Is {prof_name} strict with their grading?",
                      "What is the summary of the reviews for {prof_name}?",
                      "Would you recommend {prof_name}?",]
    
    # Select 10 professors randomly from the list
    profs = random.sample(prof_list, 10)

    final_queries = {}

    logging.info(f"Generating queries for {len(profs)} professors")

    for prof in profs:
        for query in query_template:
            if query not in final_queries:
                final_queries[query] = []
            final_queries[query].append(query.format(prof_name=prof))

    context['ti'].xcom_push(key="final_queries", value=final_queries)

    logging.info(f"Generated {len(final_queries)} queries")

    return final_queries

def get_bq_data_for_profs(**context):

    df = pd.DataFrame(columns=['question', 'context', 'query_bucket'])

    queries = context['ti'].xcom_pull(task_ids='get_bucketed_queries_task', key='final_queries')

    logging.info(f"Getting data for {len(queries)} queries")

    for query, query_list in queries.items():
        relavant_data = get_data_from_bigquery(query_list)

        for query_, data in relavant_data.items():
            df = pd.concat([df, pd.DataFrame({'question': [query_], 'context': [data['final_content']], 'query_bucket': query})], ignore_index=True)

    context['ti'].xcom_push(key='bq_data', value=df)

    return df

def generate_responses(**context):
    data = context['ti'].xcom_pull(task_ids='get_bq_data_for_profs_task', key='bq_data')
    
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
    
    data_df = pd.DataFrame(columns=['question', 'context', 'response', 'query_bucket'])
    tuned_model_endpoint = context['ti'].xcom_pull(task_ids='sft_train_task', key='tuned_model_endpoint_name')
    model = GenerativeModel(model_name=tuned_model_endpoint)
    for _, row in data.iterrows():
        llm_context = row['context']
        input_prompt = prompt.format(query=row['question'], content=llm_context)
        logging.info(f"Generating response for {row['question']}")
        llm_res = get_llm_response(input_prompt, model)
        data_df = pd.concat([data_df, pd.DataFrame({'question': [row['question']], 'context': [llm_context], 'response': [llm_res], 'query_bucket': row['query_bucket']})], ignore_index=True)

    context['ti'].xcom_push(key='llm_responses', value=data_df)

    return data_df

def get_sentiment_score(**context):
    prompt = """
                Given the response from a fine tuned model, evaluate the sentiment of the response:

                {response}

                Here are some examples of sentiment scores:

                5: (Very Positive) The response is overwhelmingly positive, with a strong endorsement or positive sentiment.
                4: (Positive) The response is generally positive, with a favorable tone and supportive language.
                3: (Neutral) The response is balanced and neutral, providing information without strong sentiment.
                2: (Negative) The response is generally negative, with critical or unfavorable language.
                1: (Very Negative) The response is overwhelmingly negative, with strong criticism or negative sentiment.

                # Few shot examples:
                # 1. "This course is amazing, I would recommend it to anyone." - Sentiment: 5
                # 2. "The course was okay, but the instructor was not very engaging." - Sentiment: 3
                # 3. "I would not recommend this course to anyone, it was a waste of time." - Sentiment: 1

                Just provide the sentiment score (1-5) based on the response.
                Just return the sentiment score as an integer.

            """
    data = context['ti'].xcom_pull(task_ids='generate_responses_task', key='llm_responses')
    model = GenerativeModel(model_name="gemini-1.5-flash-002")

    data_df = pd.DataFrame(columns=['question', 'context', 'response', 'query_bucket', 'sentiment_score'])

    for _, row in data.iterrows():
        input_prompt = prompt.format(response=row['response'])
        logging.info(f"Getting sentiment score for {row['question']}")
        llm_res = get_llm_response(input_prompt, model)
        logging.info(f"Response: {llm_res}")
        sentiment_score = int(llm_res)
        data_df = pd.concat([data_df, pd.DataFrame({'question': [row['question']], 'context': [row['context']], 'response': [row['response']], 'query_bucket': row['query_bucket'], 'sentiment_score': [sentiment_score]})], ignore_index=True)

    context['ti'].xcom_push(key='sentiment_score_data', value=data_df)

    return data_df


def generate_bias_report(**context):
    data = context['ti'].xcom_pull(task_ids='get_sentiment_score_task', key='sentiment_score_data')
    logging.info(f"Generating bias report for {len(data)} responses")

    # Calculate the mean, std, and count of sentiment scores for each query bucket
    report_df = data.groupby('query_bucket')['sentiment_score'].agg(['mean', 'std', 'count']).reset_index()

    context['ti'].xcom_push(key='bias_report', value=report_df)

    return report_df

def save_bias_report(**context):
    report = context['ti'].xcom_pull(task_ids='generate_bias_report_task', key='bias_report')
    report.to_csv('tmp/bias_report.csv', index=False)
    context['ti'].xcom_push(key='bias_report_path', value='tmp/bias_report.csv')


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

        final_content = '\n\n'.join(result_content)
        if len(final_content) >= 100000:
            final_content = final_content[:100000]
        query_response[new_query] = {
            'crns': result_crns,
            'final_content': final_content
        }


    return query_response

def save_eval_data(data, filename):
    with open(filename, 'w') as f:
        f.write(data)

    logging.info(f"Eval data saved to {filename}")
    return filename


    





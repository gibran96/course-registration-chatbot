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

from model_scripts.constants.prompts import GET_SENTIMENT_PROMPT, INSTRUCTION_PROMPT
from model_scripts.constants.queries import QUERIES_FOR_EVAL
from model_scripts.constants.sql_queries import EMBEDDINGS_QUERY
from model_scripts.utils.email_triggers import send_bias_detected_mail


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
    """
    Retrieves a list of unique professor names from the BigQuery table specified by Variable.get('banner_table_name').

    Args:
        **context: Airflow context dictionary containing task instance (ti) and other metadata.

    Returns:
        list: A list of distinct professor names retrieved from the BigQuery table.
    """
    client = bigquery.Client()

    prof_query = """
        SELECT DISTINCT faculty_name
        FROM `{}`""".format(Variable.get('banner_table_name'))
    
    prof_list = list(set(row["faculty_name"] for row in client.query(prof_query).result()))
    logging.info(f"Found {len(prof_list)} unique professors")
    context['ti'].xcom_push(key='prof_list', value=prof_list)
    return prof_list

def parse_response(response):
    """
    Extracts and parses a JSON object from a response string.

    The response is expected to contain a JSON object enclosed in triple backticks.
    This function searches for the JSON object, parses it, and returns it as a Python object.

    :param response: The string containing the response with the JSON object.
    :return: A Python object parsed from the JSON object found in the response.
    :raises ValueError: If no JSON object is found in the response.
    """
    matches = re.findall(r'```json(.*?)```', response, re.DOTALL)
    if matches:
        return ast.literal_eval(matches[0])
    else:
        raise ValueError("No JSON object found in response")
    


def get_bucketed_queries(**context):
    """
    Generates a set of queries for evaluation of the LLM model.

    Selects 10 professors randomly from the list of professors and generates a set of queries
    for each professor using the query templates. The generated queries are stored in XCom
    for further use in the DAG.

    :param context: Airflow context dictionary containing task instance (ti) and other metadata.
    :return: A dictionary with query templates as keys and a list of dictionaries containing the
             generated query and the professor name as values.
    """
    prof_list = context['ti'].xcom_pull(task_ids='get_unique_profs_task', key='prof_list')
    query_template = QUERIES_FOR_EVAL
    
    # Select 10 professors randomly from the list
    profs = random.sample(prof_list, 10)

    final_queries = {}

    logging.info(f"Generating queries for {len(profs)} professors")

    for prof in profs:
        for query in query_template:
            if query not in final_queries:
                final_queries[query] = []
            final_queries[query].append({'query': query.format(prof_name=prof), 'prof_name': prof})

    context['ti'].xcom_push(key="final_queries", value=final_queries)

    logging.info(f"Generated {len(final_queries)} queries")

    return final_queries

def get_bq_data_for_profs(**context):

    """
    Retrieves data from BigQuery for the generated queries.

    Pulls the list of generated queries from XCom and retrieves data from BigQuery
    for each query. The retrieved data is stored in a Pandas DataFrame and pushed
    to XCom for further use in the DAG.

    :param context: Airflow context dictionary containing task instance (ti) and other metadata.
    :return: The retrieved data as a Pandas DataFrame.
    """
    df = pd.DataFrame(columns=['question', 'context', 'query_bucket', 'prof_name'])

    queries = context['ti'].xcom_pull(task_ids='get_bucketed_queries_task', key='final_queries')

    logging.info(f"Getting data for {len(queries)} queries")

    for query, query_list in queries.items():
        relavant_data = get_data_from_bigquery(query_list)

        for query_, data in relavant_data.items():
            df = pd.concat([df, pd.DataFrame({'question': [query_], 'context': [data['final_content']], 'query_bucket': query, 'prof_name': query_list['prof_name'] })], ignore_index=True)

    context['ti'].xcom_push(key='bq_data', value=df)

    return df

def generate_responses(**context):
    """
    Generates LLM responses for the data retrieved from BigQuery.

    Pulls the list of generated queries from XCom and retrieves data from BigQuery
    for each query. The retrieved data is stored in a Pandas DataFrame and pushed
    to XCom for further use in the DAG.

    :param context: Airflow context dictionary containing task instance (ti) and other metadata.
    :return: The retrieved data as a Pandas DataFrame.
    """
    data = context['ti'].xcom_pull(task_ids='get_bq_data_for_profs_task', key='bq_data')
    prompt = INSTRUCTION_PROMPT
    
    data_df = pd.DataFrame(columns=['question', 'context', 'response', 'query_bucket', 'prof_name'])
    tuned_model_endpoint = context['ti'].xcom_pull(task_ids='sft_train_task', key='tuned_model_endpoint_name')
    model = GenerativeModel(model_name=tuned_model_endpoint)
    for _, row in data.iterrows():
        llm_context = row['context']
        input_prompt = prompt.format(query=row['question'], content=llm_context)
        logging.info(f"Generating response for {row['question']}")
        llm_res = get_llm_response(input_prompt, model)
        data_df = pd.concat([data_df, pd.DataFrame({'question': [row['question']], 'context': [llm_context], 'response': [llm_res], 'query_bucket': row['query_bucket'], 'prof_name': row['prof_name']})], ignore_index=True)

    context['ti'].xcom_push(key='llm_responses', value=data_df)

    return data_df

def get_sentiment_score(**context):
    """
    Retrieves the sentiment score for the responses generated by the LLM.

    Pulls the DataFrame of generated responses from XCom and uses a GenerativeModel to
    evaluate the sentiment score for each response. The sentiment score is then added to
    the DataFrame and pushed to XCom for further use in the DAG.

    :param context: Airflow context dictionary containing task instance (ti) and other metadata.
    :return: The DataFrame with the added sentiment scores.
    """
    prompt = GET_SENTIMENT_PROMPT
    data = context['ti'].xcom_pull(task_ids='generate_responses_task', key='llm_responses')
    model = GenerativeModel(model_name="gemini-1.5-flash-002")

    data_df = pd.DataFrame(columns=['question', 'context', 'response', 'query_bucket', 'sentiment_score', 'prof_name'])

    for _, row in data.iterrows():
        input_prompt = prompt.format(response=row['response'])
        logging.info(f"Getting sentiment score for {row['question']}")
        llm_res = get_llm_response(input_prompt, model)
        sentiment_score = parse_response(llm_res)
        data_df = pd.concat([data_df, pd.DataFrame({'question': [row['question']], 'context': [row['context']], 'response': [row['response']], 'query_bucket': row['query_bucket'], 'sentiment_score': [sentiment_score], 'prof_name': row['prof_name']})], ignore_index=True)

    context['ti'].xcom_push(key='sentiment_score_data', value=data_df)

    return data_df


def generate_bias_report(**context):
    """
    Generates a bias report based on the sentiment scores of LLM responses.

    Pulls the sentiment scores from XCom and calculates the mean, standard deviation, and count of sentiment scores for each query bucket and professor.
    The report is then pushed to XCom as two separate DataFrames, one for each grouping.
    If there are any biased query buckets or professors, the report is sent via email.
    """
    data = context['ti'].xcom_pull(task_ids='get_sentiment_score_task', key='sentiment_score_data')
    logging.info(f"Generating bias report for {len(data)} responses")

    # Calculate the mean, std, and count of sentiment scores for each query bucket and include prof_name column
    report_df_by_query = data.groupby(['query_bucket'])['sentiment_score'].agg(['mean', 'std', 'count']).reset_index()

    report_df_by_prof = data.groupby(['prof_name'])['sentiment_score'].agg(['mean', 'std', 'count']).reset_index()

    context['ti'].xcom_push(key='bias_report_prof', value=report_df_by_prof)
    context['ti'].xcom_push(key='bias_report_query', value=report_df_by_query)
    
    bias_prof = report_df_by_prof[(report_df_by_prof['mean'] > 4) | (report_df_by_prof['mean'] < 2)]
    bias_query = report_df_by_query[(report_df_by_query['mean'] > 4) | (report_df_by_query['mean'] < 2)]
    
    if (len(bias_prof) > 0) or (len(bias_query) > 0):
        logging.info(f"Bias detected in {len(bias_prof)} professors and {len(bias_query)} query buckets")
        send_bias_detected_mail(bias_prof, bias_query, report_df_by_query,)
    else:
        logging.info("No bias detected")
    
    return report_df_by_query, report_df_by_prof

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
    """
    Retrieves data from BigQuery based on the given queries.

    Retrieves data from BigQuery using the EMBEDDINGS_QUERY and given queries. The data is
    processed by removing punctuation from the full_info column and then joining all the
    contents together. If the length of the final content is greater than 100000, it is
    truncated to 100000 characters. The retrieved data is stored in a dictionary where the
    key is the query and the value is a dictionary containing the list of CRNs and the
    processed final content.

    Parameters
    ----------
    queries : list
        The list of queries to retrieve data from BigQuery.

    Returns
    -------
    dict
        A dictionary where the key is the query and the value is a dictionary containing the
        list of CRNs and the processed final content.
    """
    client = bigquery.Client()
    query_response = {}

    for new_query in queries:
        logging.info(f"Processing query: {new_query}")
        bq_query = EMBEDDINGS_QUERY
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

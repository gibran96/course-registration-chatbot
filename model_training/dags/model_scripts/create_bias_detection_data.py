import os
import vertexai
from google.cloud import bigquery
from vertexai.generative_models import GenerationConfig, GenerativeModel
from google.generativeai.types import HarmCategory, HarmBlockThreshold
from airflow.models import Variable
import re
import ast
import random
import logging
import string

PROJECT_ID = "coursecompass"

vertexai.init(project=PROJECT_ID, location="us-central1")

class GeminiAPI:
    def __init__(self, model_name="gemini-1.5-pro-002"):
        self.model = GenerativeModel(model_name)
        logging.info(f"Initializing model: {model_name}")

    def generate(
        self, input_prompt, temperature=1.7, top_p=0.8, top_k=100, max_tokens=2048
    ):
        response = self.model.generate_content(
            input_prompt,
            generation_config=GenerationConfig(
                max_output_tokens=max_tokens,
                top_k=top_k,
                top_p=top_p,
                temperature=temperature,
            ),
        )
        return response.text
    
gemini_api = GeminiAPI()

def get_unique_profs():
    client = bigquery.Client()

    prof_query = """
        SELECT DISTINCT faculty_name
        FROM `{}`""".format(Variable.get('banner_table_name'))
    
    prof_list = list(set(row["faculty_name"] for row in client.query(prof_query).result()))
    logging.info(f"Found {len(prof_list)} unique professors")
    return prof_list

def parse_response(response):
    matches = re.findall(r'```json(.*)```', response, re.DOTALL)
    if matches:
        return ast.literal_eval(matches[0])
    else:
        raise ValueError("No JSON object found in response")
    

def get_bucketed_profs(prof_list):
    
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
    response = gemini_api.generate(prompt.format(prof_list=prof_list))
    logging.info("parsing prof list")
    new_prof_list = parse_response(response)
    return new_prof_list

def get_bucketed_queries(prof_list):
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
    return male_queries, female_queries



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

        # logging.info(f"Similarity search results for query '{new_query}': {','.join(result_crns)}")

    return query_response



def get_eval_datasets(prof_list):
    male_queries, female_queries = get_bucketed_queries(prof_list)
    logging.info(f"male queries: {len(male_queries)}")
    logging.info(f"female queries: {len(female_queries)}")
    male_data = get_data_from_bigquery(male_queries)
    female_data = get_data_from_bigquery(female_queries)
    return male_data, female_data




    
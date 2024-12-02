import logging
from google.cloud import bigquery
from airflow.models import Variable
import logging

logging.basicConfig(level=logging.INFO)

def get_train_queries_from_bq(**context):
    """
    Retrieves 
    """
    client = bigquery.Client()
    
    train_data_query = """
        SELECT DISTINCT question
        FROM `{}`""".format(Variable.get('train_data_table_name'))
    
    question_list = list(set(row["question"] for row in client.query(train_data_query).result()))
    
    logging.info(f"Found {len(question_list)} unique train questions")
    
    context['ti'].xcom_push(key='train_questions', value=question_list)
    return question_list

def get_new_queries(**context):
    """
    Retrieves 
    """
    client = bigquery.Client()
    
    test_data_query = """
        SELECT DISTINCT question
        FROM `{}`""".format(Variable.get('user_data_table_name'))
    
    question_list = list(set(row["question"] for row in client.query(test_data_query).result()))
    
    logging.info(f"Found {len(question_list)} unique test questions")
    
    context['ti'].xcom_push(key='test_questions', value=question_list)
    return question_list


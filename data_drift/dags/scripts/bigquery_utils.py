import logging
from google.cloud import bigquery
from airflow.models import Variable
import logging
from google.cloud import bigquery

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
    
    context['ti'].xcom_push(key='questions', value=question_list)
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
    
    context['ti'].xcom_push(key='questions', value=question_list)
    return question_list



def move_data_from_user_table(**context):
    client = bigquery.Client()

    source_table_ref = Variable.get('user_data_table_name')
    destination_table_ref = Variable.get('historic_user_data_table_name')

    insert_query = f"""
    INSERT INTO {destination_table_ref}
    SELECT * FROM {source_table_ref};
    """
    query_job = client.query(insert_query)
    query_job.result()  

    delete_query = f"""
    DELETE FROM {source_table_ref}
    WHERE TRUE;
    """
    query_job = client.query(delete_query)
    query_job.result() 

    logging.info(f"Data moved from {source_table_ref} to {destination_table_ref} and deleted from source.")


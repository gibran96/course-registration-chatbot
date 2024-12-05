
from datetime import datetime, timedelta
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import logging
from scripts.backoff import exponential_backoff
from scripts.bigquery_utils_data_drift import insert_drift_history_into_table, fetch_drift_history
from airflow.models import Variable

logging.basicConfig(level=logging.INFO)


@exponential_backoff()
def get_embeddings(model, model_inputs):
    embeddings = model.get_embeddings(model_inputs)
    return embeddings


def get_train_embeddings(**context):
    train_questions = context['ti'].xcom_pull(task_ids='get_train_questions', key='questions')

    task = "CLUSTERING"
    model = TextEmbeddingModel.from_pretrained("text-embedding-005")
    batch_size = 4
    embeddings = []
    query_inputs = [TextEmbeddingInput(question, task) for question in train_questions]
    logging.info("Getting train embeddings")
    for i in range(0, len(query_inputs), batch_size):
        query_embeddings = get_embeddings(model, query_inputs[i:i+batch_size])
        embeddings.extend([embedding.values for embedding in query_embeddings])
    logging.info(f"Got train embeddings {len(query_embeddings)}")
    logging.info(f"Sample embeddings: {embeddings[0]}")
    context['ti'].xcom_push(key='train_embeddings', value=embeddings)
    return embeddings

def get_test_embeddings(**context):
    test_questions = context['ti'].xcom_pull(task_ids='get_test_questions', key='questions')

    task = "CLUSTERING"
    model = TextEmbeddingModel.from_pretrained("text-embedding-005")
    batch_size = 4
    query_inputs = [TextEmbeddingInput(question, task) for question in test_questions]
    embeddings = []
    logging.info("Getting test embeddings")
    for i in range(0, len(query_inputs), batch_size):
        query_embeddings = get_embeddings(model, query_inputs[i:i+batch_size])
        embeddings.extend([embedding.values for embedding in query_embeddings])
    logging.info(f"Got test embeddings {len(query_embeddings)}")
    logging.info(f"Sample embeddings: {embeddings[0]}")
    context['ti'].xcom_push(key='test_embeddings', value=embeddings)
    return embeddings


def get_thresholds(**context):
    train_embeddings = context['ti'].xcom_pull(task_ids='get_train_embeddings', key='train_embeddings')

    ## batched cosine similarity
    batch_size = 4
    minimum_sim = np.inf
    for i in range(0, len(train_embeddings), batch_size):
        cosine_similarities = cosine_similarity(train_embeddings[i:i+batch_size])
        min_cosine_sim = cosine_similarities.min()
        minimum_sim = min(minimum_sim, min_cosine_sim)

    upper_threshold = minimum_sim - (minimum_sim * 0.1)
    lower_threshold = minimum_sim - (minimum_sim * 0.6)

    context['ti'].xcom_push(key='upper_threshold', value=upper_threshold)
    context['ti'].xcom_push(key='lower_threshold', value=lower_threshold)

    return (upper_threshold, lower_threshold)

def detect_data_drift(**context):
    test_embeddings = context['ti'].xcom_pull(task_ids='get_test_embeddings', key='test_embeddings')
    train_embeddings = context['ti'].xcom_pull(task_ids='get_train_embeddings', key='train_embeddings')
    test_questions = context['ti'].xcom_pull(task_ids='get_test_questions', key='questions')

    upper_threshold = context['ti'].xcom_pull(task_ids='get_thresholds', key='upper_threshold')
    lower_threshold = context['ti'].xcom_pull(task_ids='get_thresholds', key='lower_threshold')

    data_drift = False
    drift_queries = []
    detected_drift_queries = []

    ## batched cosine similarity
    batch_size = 4
    
    # Iterate through test embeddings
    for i in range(0, len(test_embeddings), batch_size):
        batch_test_embeddings = test_embeddings[i:i+batch_size]
        batch_test_questions = test_questions[i:i+batch_size]
        
        for j, test_embedding in enumerate(batch_test_embeddings):
            # Check similarity with all train embeddings
            similarities = cosine_similarity([test_embedding], train_embeddings)[0]
            min_similarity = similarities.min()
            
            # If similarity is below upper threshold but above lower threshold
            if (min_similarity < upper_threshold) and (min_similarity > lower_threshold):
                data_drift = True
                detected_drift_queries.append({
                    'query': batch_test_questions[j],
                    'similarity': min_similarity
                })

    # Log detailed results
    if data_drift:
        logging.info(f"Data drift detected in {len(detected_drift_queries)} specific queries")
        for drift_info in detected_drift_queries:
            logging.info(f"Drift Query: '{drift_info['query']}' (Max Similarity: {drift_info['similarity']:.4f})")
        insert_drift_history_into_table(detected_drift_queries)
    else:
        logging.info("No data drift detected")
    
    context['ti'].xcom_push(key='data_drift', value=data_drift)
    context['ti'].xcom_push(key='detected_drift_queries', value=detected_drift_queries)

    return data_drift

def check_drift_trend(**context):
    drift_last_detected_at = Variable.get('drift_last_detected_at')
    # Set drift window for max of date 7 days ago or last detected drift
    date_seven_days_ago = datetime.now() - timedelta(days=7)
    date_seven_days_ago = date_seven_days_ago.strftime("%Y-%m-%d %H:%M:%S")

    query_condition = drift_last_detected_at
    if drift_last_detected_at < date_seven_days_ago:
        query_condition = date_seven_days_ago

    detected_drift_queries = fetch_drift_history(query_condition)
    logging.info(f"Detected drift queries: {len(detected_drift_queries)}")

    if len(detected_drift_queries) >= 2:
        logging.info("Data drift trend detected. Triggering train_data_dag")
        drift_queries = [query['query'] for query in detected_drift_queries]
        context['ti'].xcom_push(key='drift_queries', value=drift_queries)
        return 'perform_similarity_search'
    else:
        logging.info("No data drift trend detected")
        return 'dummy_task'
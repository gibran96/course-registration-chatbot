
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import logging
from scripts.backoff import exponential_backoff

logging.basicConfig(level=logging.INFO)


@exponential_backoff()
def get_embeddings(model, model_inputs):
    embeddings = model.get_embeddings(model_inputs)
    return embeddings


def get_train_embeddings(**context):
    train_questions = context['ti'].xcom_pull(task_ids='get_train_questions', key='questions')

    task = "CLUSTERING"
    model = TextEmbeddingModel.from_pretrained("text-embedding-005")
    batch_size = 8
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
    batch_size = 8
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
    train_embeddings = context['ti'].xcom_pull(task_ids='get_train_embeddings', key='task_status')

    ## batched cosine similarity
    batch_size = 8
    minimum_sim = np.inf
    for i in range(0, len(train_embeddings), batch_size):
        cosine_similarities = cosine_similarity(train_embeddings[i:i+batch_size])
        min_cosine_sim = min(cosine_similarities)
        minimum_sim = min(minimum_sim, min_cosine_sim)

    upper_threshold = minimum_sim - (minimum_sim * 0.1)
    lower_threshold = minimum_sim - (minimum_sim * 0.6)

    context['ti'].xcom_push(key='upper_threshold', value=upper_threshold)
    context['ti'].xcom_push(key='lower_threshold', value=lower_threshold)

    return (upper_threshold, lower_threshold)

def detect_data_drift(**context):
    test_embeddings = context['ti'].xcom_pull(task_ids='get_test_embeddings', key='test_embeddings')
    train_embeddings = context['ti'].xcom_pull(task_ids='get_train_embeddings', key='train_embeddings')

    upper_threshold = context['ti'].xcom_pull(task_ids='get_thresholds', key='upper_threshold')
    lower_threshold = context['ti'].xcom_pull(task_ids='get_thresholds', key='lower_threshold')

    data_drift = False

    ## batched cosine similarity
    batch_size = 8
    minimum_sim = np.inf
    lowest_sim = np.inf
    for i in range(0, len(test_embeddings), batch_size):
        for j in range(0, len(train_embeddings), batch_size):
            cosine_similarities = cosine_similarity(train_embeddings[j:j+batch_size], test_embeddings[i:i+batch_size])
            min_cosine_sim = min(cosine_similarities)
            if min_cosine_sim > lower_threshold:
                minimum_sim = min(minimum_sim, min_cosine_sim)
            lowest_sim = min(lowest_sim, min_cosine_sim)

    if (minimum_sim < upper_threshold) and (minimum_sim > lower_threshold):
        data_drift = True
        logging.info(f"Data drift detected: {minimum_sim} < {upper_threshold} and {minimum_sim} > {lower_threshold}")
    else:
        logging.info(f"No data drift detected")
    if lowest_sim < lower_threshold:
        logging.info(f"Outliers detected: {lowest_sim} < {lower_threshold}")
    
    context['ti'].xcom_push(key='data_drift', value=data_drift)

    return data_drift


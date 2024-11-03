from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models import Variable
from google.cloud import storage, bigquery
from scripts.data_utils import upload_to_gcs  # Reusing existing utility
from scripts.fetch_banner_data import get_cookies, get_courses_list, get_course_description
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import openai  # Assuming OpenAI for LLM
import logging
import json

logging.basicConfig(level=logging.INFO)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

def get_existing_course_data(**context):
    """Fetch existing course data from BigQuery"""
    client = bigquery.Client()
    query = f"""
        SELECT DISTINCT crn, course_name, professor, description
        FROM `{Variable.get('course_table_name')}`
    """
    df = client.query(query).to_dataframe()
    context['task_instance'].xcom_push(key='course_data', value=df.to_dict('records'))
    return df.to_dict('records')

def extract_pdf_data(**context):
    """Extract data from PDFs stored in GCS"""
    bucket_name = Variable.get('default_bucket_name')
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    # List all PDFs in the specified folder
    blobs = bucket.list_blobs(prefix='course_review_dataset/')
    
    extracted_data = []
    for blob in blobs:
        if blob.name.endswith('.pdf'):
            # Download PDF content
            content = blob.download_as_bytes()
            # Extract text and metadata
            # You'll need to implement PDF extraction logic here
            extracted_data.append({
                'pdf_name': blob.name,
                'content': content,
                'metadata': blob.metadata
            })
    
    context['task_instance'].xcom_push(key='extracted_pdf_data', value=extracted_data)
    return extracted_data

def perform_similarity_search(**context):
    """
    Perform similarity search between course-prof pairs and PDF content
    """
    # Get course data and PDF content from XCom
    course_data = context['task_instance'].xcom_pull(key='course_data')
    pdf_data = context['task_instance'].xcom_pull(key='extracted_pdf_data')
    
    # Prepare text data for similarity comparison
    vectorizer = TfidfVectorizer(stop_words='english')
    
    results = []
    for course in course_data:
        # Create course query string
        course_query = f"{course['course_name']} {course['professor']} {course['description']}"
        
        # Compare with each PDF
        for pdf in pdf_data:
            # Vectorize both texts
            tfidf_matrix = vectorizer.fit_transform([course_query, pdf['content']])
            
            # Calculate similarity
            similarity = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])[0][0]
            
            if similarity > 0.3:  # Threshold for relevance
                results.append({
                    'crn': course['crn'],
                    'course_name': course['course_name'],
                    'professor': course['professor'],
                    'pdf_name': pdf['pdf_name'],
                    'similarity_score': similarity,
                    'relevant_content': pdf['content'][:1000]  # First 1000 chars for context
                })
    
    context['task_instance'].xcom_push(key='similarity_results', value=results)
    return results

def generate_llm_response(**context):
    """
    Generate LLM response based on similarity search results
    """
    similarity_results = context['task_instance'].xcom_pull(key='similarity_results')
    openai.api_key = Variable.get('openai_api_key')
    
    analyzed_results = []
    for result in similarity_results:
        prompt = f"""
        Based on the following course information and related content, provide a detailed analysis:
        
        Course: {result['course_name']}
        Professor: {result['professor']}
        
        Related Content:
        {result['relevant_content']}
        
        Please provide:
        1. A summary of the course content and teaching style
        2. Key strengths and potential areas for improvement
        3. Overall assessment and recommendations
        
        Format the response as JSON with these keys: summary, strengths, improvements, assessment
        """
        
        try:
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are an expert course analyzer."},
                    {"role": "user", "content": prompt}
                ]
            )
            
            analysis = json.loads(response.choices[0].message.content)
            analyzed_results.append({
                'crn': result['crn'],
                'course_name': result['course_name'],
                'professor': result['professor'],
                'analysis': analysis,
                'source_pdf': result['pdf_name'],
                'confidence_score': result['similarity_score']
            })
        except Exception as e:
            logging.error(f"Error generating LLM response for {result['course_name']}: {str(e)}")
    
    context['task_instance'].xcom_push(key='llm_results', value=analyzed_results)
    return analyzed_results

def process_llm_output(**context):
    """
    Process and format LLM generated responses for storage
    """
    llm_results = context['task_instance'].xcom_pull(key='llm_results')
    
    # Prepare data for BigQuery
    processed_results = []
    for result in llm_results:
        processed_results.append({
            'crn': result['crn'],
            'course_name': result['course_name'],
            'professor': result['professor'],
            'summary': result['analysis']['summary'],
            'strengths': result['analysis']['strengths'],
            'improvements': result['analysis']['improvements'],
            'assessment': result['analysis']['assessment'],
            'source_pdf': result['source_pdf'],
            'confidence_score': result['confidence_score'],
            'analysis_timestamp': datetime.now().isoformat()
        })
    
    # Save to temporary CSV for BigQuery upload
    df = pd.DataFrame(processed_results)
    df.to_csv('/tmp/llm_analysis_results.csv', index=False)
    
    # Upload to GCS
    upload_to_gcs(
        bucket_name=Variable.get('default_bucket_name'),
        source_path='/tmp/llm_analysis_results.csv',
        destination_path='llm_analysis/results.csv'
    )
    
    return processed_results

with DAG(
    'course_review_llm_pipeline',
    default_args=default_args,
    description='Process course reviews with LLM analysis',
    schedule_interval='0 0 * * *',  # Daily at midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['pdf', 'banner', 'llm'],
    max_active_runs=1
) as dag:

    # Get existing course data
    get_course_data_task = PythonOperator(
        task_id='get_course_data',
        python_callable=get_existing_course_data,
        provide_context=True,
    )

    # Extract PDF data
    extract_pdf_task = PythonOperator(
        task_id='extract_pdf_data',
        python_callable=extract_pdf_data,
        provide_context=True,
    )

    # Perform similarity search
    similarity_search_task = PythonOperator(
        task_id='perform_similarity_search',
        python_callable=perform_similarity_search,
        provide_context=True,
    )

    # Generate LLM responses
    generate_llm_response_task = PythonOperator(
        task_id='generate_llm_response',
        python_callable=generate_llm_response,
        provide_context=True,
    )

    # Process LLM output
    process_output_task = PythonOperator(
        task_id='process_llm_output',
        python_callable=process_llm_output,
        provide_context=True,
    )

    # Load results to BigQuery
    load_to_bigquery_task = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket=Variable.get('default_bucket_name'),
        source_objects=['llm_analysis/results.csv'],
        destination_project_dataset_table=Variable.get('llm_results_table'),
        write_disposition='WRITE_APPEND',
        autodetect=True,
        skip_leading_rows=1,
    )

    # Define task dependencies
    [get_course_data_task, extract_pdf_task] >> similarity_search_task >> generate_llm_response_task >> process_output_task >> load_to_bigquery_task
import pandas as pd
import os
import logging
from google.cloud import storage
from airflow.models import Variable
import fitz
import gc
import re
import string
import uuid
from google.cloud import aiplatform
from datetime import datetime

aiplatform.init(project="coursecompass", location="us-east1")



# Question mapping
question_map = {
    "What were the strengths of this course and/or this instructor?": "Q1",
    "What could the instructor do to make this course better?": "Q2",
    "Please expand on the instructor's strengths and/or areas for improvement in facilitating inclusive learning.": "Q3",
    "Please comment on your experience of the online course environment in the open-ended text box.": "Q4",
    "What I could have done to make this course better for myself.": "Q5"
}

def clean_response(response):
    """Remove leading index numbers from a response."""
    response = re.sub(r"^\d+\s*", "", response.strip())
    # remove next line characters
    response = response.replace("\n", " ")
    response = response.replace("\r", " ")
    response = response.replace("\t", " ")
    return response

def clean_text(text):
    """Clean and standardize text."""
    text = text.strip()
    text = ''.join(e for e in text if e.isalnum() or e.isspace() or e in string.punctuation)
    text = text.lower()
    text = clean_response(text)
    return text

def extract_data_from_pdf(pdf_file):
    structured_data = {
        "crn": "",
        "course_title": "",
        "course_code": "",
        "instructor": "",
        "term": "",
        "responses": []
    }

    for page_num in range(len(pdf_file)):
        page_text = pdf_file[page_num].get_text()

        # Extract course information
        if "Course ID" in page_text:
            course_id = page_text.split("Course ID: ")[1].split("\n")[0]
            instructor = page_text.split("Instructor: ")[1].split("\n")[0]
            course_title = page_text.split("\n")[0].split("(")[0].strip()
            term = page_text.split("\n")[0].split("(")[1][:-1].strip()
            course_code = page_text.split("Catalog & Section: ")[1].split(" ")[0]
            structured_data["crn"] = course_id
            structured_data["instructor"] = instructor
            structured_data["course_title"] = course_title
            structured_data["course_code"] = course_code 
            structured_data["term"] = term

        # Extract questions and responses
        if "Q:" in page_text:
            questions = page_text.split("Q: ")[1:]
            for question in questions:
                question_text = question.split("\n")[0].strip()
                question_key = question_map.get(question_text, "unknown")

                responses = question.split("\n")[1:]
                actual_responses = []
                temp_response = ""

                for response in responses:
                    response = clean_text(response)
                    
                    if response.strip().isdigit() and temp_response:
                        cleaned_response = clean_response(temp_response)
                        actual_responses.append(cleaned_response)
                        temp_response = ""
                    else:
                        temp_response += f" {response.strip()}"

                if temp_response.strip():
                    cleaned_response = clean_response(temp_response)
                    actual_responses.append(cleaned_response)

                structured_data["responses"].append({
                    "question": question_key,
                    "responses": actual_responses
                })

    return structured_data

def process_data(structured_data, reviews_df, courses_df):
    crn = structured_data["crn"]
    course_code = structured_data["course_code"]
    course_title = structured_data["course_title"]
    instructor = structured_data["instructor"]
    term = structured_data["term"]
    
    if crn not in courses_df["crn"].values:
        new_course_row = {
            "crn": crn,
            "course_code": course_code,
            "course_title": course_title,
            "instructor": instructor,
            "term": term
        }
        courses_df = courses_df._append(new_course_row, ignore_index=True)
    
    for response_data in structured_data["responses"]:
        question = response_data.get("question", "")
        responses = response_data.get("responses", [])
        
        for response in responses:
            if response:
                new_review_row = {
                    "review_id": uuid.uuid4().hex,
                    "crn": crn,
                    "question": question,
                    "response": response,
                    "term": term
                }
                reviews_df = reviews_df._append(new_review_row, ignore_index=True)
                
    return reviews_df, courses_df

def process_pdf_files(**context):
    bucket_name = context['dag_run'].conf.get('bucket_name', Variable.get('default_bucket_name'))
    output_path = context['dag_run'].conf.get('output_path', '/tmp/processed_data')
    unique_blobs = context['ti'].xcom_pull(task_ids='get_unique_blobs', key='unique_blobs')
    
    # Initialize DataFrames
    reviews_df = pd.DataFrame(columns=["review_id", "crn", "question", "response", "term"])
    courses_df = pd.DataFrame(columns=["crn", "course_code", "course_title", "instructor"])

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix='course_review_dataset/')
    
    try:
        logging.info("Processing PDFs...")
        for blob in blobs:
            if blob.name.endswith('.pdf') and blob.name.split('/')[-1].replace('.pdf', '') in unique_blobs:
                logging.info(f"Processing {blob.name}")
                
                # Download and process PDF
                pdf_bytes = blob.download_as_bytes()
                pdf_file = fitz.open(stream=pdf_bytes, filetype="pdf")
                
                # Extract and process data
                structured_data = extract_data_from_pdf(pdf_file)
                reviews_df, courses_df = process_data(structured_data, reviews_df, courses_df)
                
                pdf_file.close()
                gc.collect()
                
                logging.info(f"Processed {blob.name}")
        
        # print length of the dataframes
        logging.info(f"Length of reviews: {reviews_df.shape[0]}")
        logging.info(f"Length of courses: {courses_df.shape[0]}")
        
        # Save processed data
        os.makedirs(output_path, exist_ok=True)
        reviews_df.to_csv(f"{output_path}/reviews.csv", index=False)
        courses_df.to_csv(f"{output_path}/courses.csv", index=False)
        
        # Create question mapping
        question_df = pd.DataFrame(list(question_map.items()), columns=["q_desc", "q_num"])
        question_df.to_csv(f"{output_path}/question_mapping.csv", index=False)
        
        return {
            'reviews_count': len(reviews_df),
            'courses_count': len(courses_df)
        }
        
    except Exception as e:
        logging.error(f"Error processing PDFs: {str(e)}")
        raise

def track_preprocessing_metadata(**context):
    """
    Track preprocessing metadata using Vertex AI Metadata.
    Returns the metadata API client and metadata objects.
    """
    # Initialize Vertex AI Metadata
    metadata_client = aiplatform.Metadata()
    
    # Create dataset metadata
    dataset_metadata = {
        'timestamp': datetime.utcnow().isoformat(),
        'dag_run_id': context['dag_run'].run_id,
        'output_path': context['dag_run'].conf.get('output_path', '/tmp/processed_data')
    }
    
    # Create Metadata Schema
    schema_metadata = {
        "raw_reviews_count": 0,
        "raw_courses_count": 0,
        "processed_reviews_count": 0,
        "processed_courses_count": 0,
        "null_responses_removed": 0,
        "sensitive_data_flags": 0,
        **dataset_metadata
    }
    
    # Create experiment run
    experiment = metadata_client.create_experiment(
        display_name=f"preprocessing_run_{context['dag_run'].run_id}"
    )
    
    # Create run
    run = metadata_client.create_run(
        experiment=experiment.name,
        display_name=f"preprocessing_{datetime.utcnow().isoformat()}",
        metadata=schema_metadata
    )
    
    return metadata_client, run

def update_preprocessing_metadata(metadata_client, run, metadata_values):
    """Update preprocessing metadata with final values."""
    # Update run with final metadata values
    run.update(metadata=metadata_values)
    
    # Log metrics
    for key, value in metadata_values.items():
        if isinstance(value, (int, float)):
            run.log_metric(key, value)

def preprocess_data(**context):
    output_path = context['dag_run'].conf.get('output_path', '/tmp/processed_data')
    
    # Initialize metadata tracking
    metadata_client, run = track_preprocessing_metadata(**context)
    
    # Initialize metadata values
    metadata_values = {
        "raw_reviews_count": 0,
        "raw_courses_count": 0,
        "processed_reviews_count": 0,
        "processed_courses_count": 0,
        "null_responses_removed": 0,
        "sensitive_data_flags": 0,
        "timestamp": datetime.utcnow().isoformat(),
        "dag_run_id": context['dag_run'].run_id,
        "output_path": output_path
    }

    try:
        # Load data
        reviews_df = pd.read_csv(f"{output_path}/reviews.csv")
        courses_df = pd.read_csv(f"{output_path}/courses.csv")
        
        # Record initial counts
        metadata_values["raw_reviews_count"] = len(reviews_df)
        metadata_values["raw_courses_count"] = len(courses_df)
        
        # Log initial metrics
        run.log_metric("raw_reviews_count", len(reviews_df))
        run.log_metric("raw_courses_count", len(courses_df))

        # Preprocess data
        reviews_df["response"] = reviews_df["response"].apply(clean_text)
        courses_df["course_title"] = courses_df["course_title"].apply(clean_text)

        # Track null responses removed
        null_count = reviews_df["response"].isnull().sum()
        reviews_df = reviews_df[reviews_df["response"].notnull()]
        metadata_values["null_responses_removed"] = null_count
        run.log_metric("null_responses_removed", null_count)

        # Check for sensitive data
        flag, sensitive_data_found = check_for_gender_bias(reviews_df, "response")
        if flag:
            logging.warning("Sensitive data found in responses")
            logging.warning(sensitive_data_found)
        sensitive_count = len(sensitive_data_found) if flag else 0
        metadata_values["sensitive_data_flags"] = sensitive_count
        run.log_metric("sensitive_data_flags", sensitive_count)

        # Record final counts
        metadata_values["processed_reviews_count"] = len(reviews_df)
        metadata_values["processed_courses_count"] = len(courses_df)
        
        # Log final metrics
        run.log_metric("processed_reviews_count", len(reviews_df))
        run.log_metric("processed_courses_count", len(courses_df))

        # Save preprocessed data
        reviews_preprocessed_path = f"{output_path}/reviews_preprocessed.csv"
        courses_preprocessed_path = f"{output_path}/courses_preprocessed.csv"
        reviews_df.to_csv(reviews_preprocessed_path, index=False)
        courses_df.to_csv(courses_preprocessed_path, index=False)

        # Update metadata with final values
        update_preprocessing_metadata(metadata_client, run, metadata_values)
        
        # Log metadata summary
        logging.info(f"Preprocessing metadata: {metadata_values}")
        
        # Mark run as completed
        run.update_state(state=aiplatform.runtime.metadata.execution.State.COMPLETE)
        
        return {
            'reviews_count': len(reviews_df),
            'courses_count': len(courses_df),
            'metadata': metadata_values,
            'run_name': run.name
        }
        
    except Exception as e:
        # Mark run as failed
        run.update_state(state=aiplatform.runtime.metadata.execution.State.FAILED)
        logging.error(f"Error in preprocessing: {str(e)}")
        raise
    finally:
        # End the run
        run.end()
# def preprocess_data(**context):
#     output_path = context['dag_run'].conf.get('output_path', '/tmp/processed_data')
    


#     # Load data
#     reviews_df = pd.read_csv(f"{output_path}/reviews.csv")
#     courses_df = pd.read_csv(f"{output_path}/courses.csv")

    
#     # # Preprocess data
#     reviews_df["response"] = reviews_df["response"].apply(clean_text)
#     courses_df["course_title"] = courses_df["course_title"].apply(clean_text)

#     # Remove rows with null responses
#     reviews_df = reviews_df[reviews_df["response"].notnull()]

#     # Check for sensitive data
#     flag, sensitive_data_found = check_for_gender_bias(reviews_df, "response")

#     # if flag is True, send a notification with the sensitive data found
#     if flag:
#         logging.warning("Sensitive data found in responses")
#         logging.warning(sensitive_data_found)

#     # Save preprocessed data
#     reviews_preprocessed_path = f"{output_path}/reviews_preprocessed.csv"
#     courses_preprocessed_path = f"{output_path}/courses_preprocessed.csv"

#     # Save preprocessed data
#     reviews_df.to_csv(reviews_preprocessed_path, index=False)
#     courses_df.to_csv(courses_preprocessed_path, index=False)

    
    
#     return {
#         'reviews_count': len(reviews_df),
#         'courses_count': len(courses_df)
#     }

def check_for_gender_bias(df, column_name):
    # Check for gender bias in the df for the given column
    # Check for any gender specific pronouns in the responses and replace it with the professor.

    gender_sensitive_pronouns = ["he", "him", "his", "she", "her", "hers", "they", "them", "theirs"]

    flag = False

    sensitive_data_found = pd.DataFrame(columns=["crn", "question", "response"])
    # Keep a track of all the rows that have the gender specific pronouns
    for pron in gender_sensitive_pronouns:
        for index, row in df.iterrows():
            if pron in row[column_name]:
                flag = True
                sensitive_data_found = sensitive_data_found.append(row)

                # Replace the pronoun with the professor
                df.at[index, column_name] = row[column_name].replace(pron, "the professor")
    
    return flag, sensitive_data_found



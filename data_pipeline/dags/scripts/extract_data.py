import pandas as pd
import pymupdf
import json
import os
import logging
from google.cloud import bigquery
from google.cloud import storage
from airflow.models import Variable
import fitz
import gc
import re
import string
import uuid

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
    return response

def clean_text(text):
    """Clean and standardize text."""
    text = text.strip()
    text = ''.join(e for e in text if e.isalnum() or e.isspace() or e in string.punctuation)
    text = text.lower()
    return text

def process_data(structured_data, reviews_df, courses_df):
    crn = structured_data["crn"]
    course_code = structured_data["course_code"]
    course_title = structured_data["course_title"]
    instructor = structured_data["instructor"]
    
    if crn not in courses_df["crn"].values:
        new_course_row = {
            "crn": crn,
            "course_code": course_code,
            "course_title": course_title,
            "instructor": instructor
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
                    "response": response
                }
                reviews_df = reviews_df._append(new_review_row, ignore_index=True)
                
    return reviews_df, courses_df


def extract_data_from_pdf(pdf_file):
    structured_data = {
        "crn": "",
        "course_title": "",
        "course_code": "",
        "instructor": "",
        "responses": []
    }

    for page_num in range(len(pdf_file)):
        page_text = pdf_file[page_num].get_text()

        # Extract course information
        if "Course ID" in page_text:
            course_id = page_text.split("Course ID: ")[1].split("\n")[0]
            instructor = page_text.split("Instructor: ")[1].split("\n")[0]
            course_title = page_text.split("\n")[0].split("(")[0].strip()
            course_code = page_text.split("Catalog & Section: ")[1].split(" ")[0]
            structured_data["crn"] = course_id
            structured_data["instructor"] = instructor
            structured_data["course_title"] = course_title
            structured_data["course_code"] = course_code 

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

def process_pdf_files(**context):
    bucket_name = context['dag_run'].conf.get('bucket_name', Variable.get('default_bucket_name'))
    output_path = context['dag_run'].conf.get('output_path', '/tmp/processed_data')
    unique_blobs = context['ti'].xcom_pull(task_ids='get_unique_blobs', key='unique_blobs')
    
    # Initialize DataFrames
    reviews_df = pd.DataFrame(columns=["crn", "question", "response"])
    courses_df = pd.DataFrame(columns=["crn", "course_code", "course_title", "instructor"])

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix='course_review_dataset/')
    
    try:
        logging.info("Processing PDFs...", blobs)
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

import os
import csv
from google.cloud import bigquery
from google.cloud import storage
import pymupdf
import json
import pandas as pd


def load_trace_data_into_bigquery(event, context):
    file_name = event['name']
    bucket_name = event['bucket']

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    pdf_data = blob.download_as_bytes()
    pdf_data = pymupdf.open(pdf_data)
    extracted_data = process_pdf(pdf_data)

    bigquery_client = bigquery.Client()
    table_id = os.getenv('BQ_TABLE')  

    errors = bigquery_client.insert_rows_json(table_id, extracted_data)
    if errors:
        print(f"Errors occurred: {errors}")
    else:
        print(f"Data successfully loaded into {table_id}")

def process_pdf(pdf_file):

    data = extract_data_from_pdf(pdf_file)
    json_data = []
    ##sample
    # json_data.append({
    #     "name": parts[0].strip(),
    #     "age": int(parts[1].strip())
    # })

    return json_data

def extract_data_from_pdf(pdf_file):

    # Get the number of pages in the pdf
    num_pages = len(pdf_file)

    # Initialize the structured data dictionary
    structured_data = {
        "crn": "",
        "course_title": "",
        "responses": []
    }

    # Loop through each page of the pdf
    for page_num in range(num_pages):
        # Get the text content of the page
        page_text = pdf_file[page_num].get_text()

        # Check if the page contains course information
        if "Course ID" in page_text:
            # Extract course code, title, and description
            instructor = page_text.split("Instructor: ")[1].split("\n")[0]
            course_code = page_text.split("Course ID: ")[1].split("\n")[0]
            course_title = page_text.split("\n")[0].split("(")[0].strip()
            # course_description = page_text.split("Course Description: ")[1].split("\n")[0]

            structured_data["crn"] = course_code
            structured_data["instructor"] = instructor
            structured_data["course_title"] = course_title
            # structured_data["course_title"] = course_title
            # structured_data["course_description"] = course_description

        # Check if the page contains responses
        if "Q:" in page_text:
            # Extract questions and responses
            questions = page_text.split("Q: ")[1:]
            for question in questions:
                question_text = question.split("\n")[0]
                responses = question.split("\n")[1:]
                actual_responses = []
                temp_response = ""
                for response in responses:
                    if response.isdigit():
                        
                        actual_responses.append(temp_response)
                        temp_response = ""
                    else:
                        temp_response += response

                structured_data["responses"].append({
                    "question": question_text,
                    "responses": actual_responses
                })

    # Close the pdf file
    pdf_file.close()
    # print(json.dumps(structured_data, indent=4))

    # Use nltk to clean the text
    for response in structured_data["responses"]:
        response["question"] = clean_text(response["question"])
        response["responses"] = [clean_text(r) for r in response["responses"]]
    return structured_data

def clean_text(text):
    # Remove leading and trailing whitespaces
    text = text.strip()
    # Remove special characters
    text = ''.join(e for e in text if e.isalnum() or e.isspace())
    # Convert to lowercase
    text = text.lower()
    return text

# def process_pdfs(pdf_directory):
#     for pdf_file in os.listdir(pdf_directory):
#         if pdf_file.endswith('.pdf'):
#             pdf_path = os.path.join(pdf_directory, pdf_file)
#             courseId = pdf_file.split('.')[0]
#             print(f"Processing: {pdf_file}")
            
#             # Extract and parse text from the PDF
#             structured_data = extract_data_from_pdf(pdf_path)
#             file_path = "data/course_comments_text/{courseId}.txt".format(courseId=courseId)
#             with open(file_path, "w") as f:
#                 f.write(json.dumps(structured_data, indent=4))

# file_path = "data/course_comments"

# process_pdfs(file_path)
import gc
import os
import re
import fitz 
import pandas as pd
from google.cloud import storage
import logging

# Question map as provided
question_map = {
    "What were the strengths of this course and/or this instructor?": "Q1",
    "What could the instructor do to make this course better?": "Q2",
    "Please expand on the instructor’s strengths and/or areas for improvement in facilitating inclusive learning.": "Q3",
    "Please comment on your experience of the online course environment in the open-ended text box.": "Q4",
    "What I could have done to make this course better for myself.": "Q5"
}

# DataFrames for the required tables
reviews_df = pd.DataFrame(columns=["crn", "question", "response"])
courses_df = pd.DataFrame(columns=["crn", "course_code", "course_title", "instructor"])

# Static table for questions
question_df = pd.DataFrame(list(question_map.items()), columns=["q_desc", "q_num"])
question_df.to_csv("question_mapping.csv", index=False)

def parse_pdfs_from_gcs(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs()
    
    for blob in blobs:
        if blob.name.endswith('.pdf'):
            logging.info(f"Processing {blob.name}.")
            
            pdf_bytes = blob.download_as_bytes()
            
            pdf_file = fitz.open(stream=pdf_bytes, filetype="pdf")
            structured_data = extract_data_from_pdf(pdf_file)
            
            process_data(structured_data)
            logging.info(f"Processed {blob.name} done.")
        gc.collect()
    
    save_data_to_csv()
            


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
            # Extract questions and responses
            questions = page_text.split("Q: ")[1:]
            for question in questions:
                question_text = question.split("\n")[0].strip()
                question_key = question_map.get(question_text, "unknown")

                responses = question.split("\n")[1:]
                actual_responses = []
                temp_response = ""

                for response in responses:
                    # Detect if a line starts with a digit indicating a new response.
                    if response.strip().isdigit() and temp_response:
                        # Clean the accumulated response before appending
                        cleaned_response = clean_response(temp_response)
                        actual_responses.append(cleaned_response)
                        temp_response = ""
                    else:
                        temp_response += f" {response.strip()}"

                # Add the last response if it exists
                if temp_response.strip():
                    cleaned_response = clean_response(temp_response)
                    actual_responses.append(cleaned_response)

                structured_data["responses"].append({
                    "question": question_key,
                    "responses": actual_responses
                })

    pdf_file.close()
    return structured_data


def clean_response(response):
    """Remove leading index numbers from a response."""
    # Remove leading digits followed by a space, e.g., "1 " or "2 "
    response = re.sub(r"^\d+\s*", "", response.strip())
    return response


def clean_text(text):
    """Clean and standardize text."""
    text = text.strip()  # Remove leading/trailing whitespace
    text = ''.join(e for e in text if e.isalnum() or e.isspace())  # Remove special characters
    text = text.lower()  # Convert to lowercase
    return text


def process_data(structured_data):
    """Process structured data into DataFrame rows."""
    global reviews_df, courses_df
    crn = structured_data["crn"]
    course_code = structured_data["course_code"]
    course_title = structured_data["course_title"]
    instructor = structured_data["instructor"]
    
    # Add course details to the courses table, if not already present
    if crn not in courses_df["crn"].values:
        new_course_row = {
            "crn": crn,
            "course_code": course_code,
            "course_title": course_title,
            "instructor": instructor
        }
        courses_df = courses_df._append(new_course_row, ignore_index=True)
    
    # Add each response to the reviews table
    for response_data in structured_data["responses"]:
        question = response_data.get("question", "")
        responses = response_data.get("responses", [])
        
        for response in responses:
            if response:  # Ignore empty responses
                new_review_row = {
                    "crn": crn,
                    "question": question,
                    "response": response
                }
                reviews_df = reviews_df._append(new_review_row, ignore_index=True)


def save_data_to_csv():
    """Save the data to CSV files."""
    reviews_df.to_csv("course_reviews.csv", index=False)
    courses_df.to_csv("courses.csv", index=False)


parse_pdfs_from_gcs("course-reviews-pdfs")
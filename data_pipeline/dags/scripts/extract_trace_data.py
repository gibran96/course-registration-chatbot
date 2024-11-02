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
from ml_metadata import metadata_store
from ml_metadata.proto import metadata_store_pb2
from airflow.models import Variable


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
    text = ''.join(e for e in text if e.isalnum() or e.isspace())
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


def setup_mlmd():
    """
    Setup ML Metadata connection and create necessary types.
    """
    # Create connection config for metadata store
    config = metadata_store_pb2.ConnectionConfig()
    config.mysql.host = Variable.get("metadata_db_host")
    config.mysql.port = 3306
    config.mysql.database = Variable.get("metadata_db_name")
    config.mysql.user = Variable.get("metadata_db_user")
    config.mysql.password = Variable.get("metadata_db_password")

    logging.info(f"Connecting to MLMD: {config}")
    

# Initialize the metadata store
    store = metadata_store.MetadataStore(config)

    # Create ArtifactTypes if they don't exist
    try:
        preprocessing_dataset_type = store.get_artifact_type("PreprocessingDataset")
    except:
        preprocessing_dataset_type = metadata_store_pb2.ArtifactType()
        preprocessing_dataset_type.name = "PreprocessingDataset"
        preprocessing_dataset_type.properties["raw_reviews_count"] = metadata_store_pb2.INT
        preprocessing_dataset_type.properties["raw_courses_count"] = metadata_store_pb2.INT
        preprocessing_dataset_type.properties["processed_reviews_count"] = metadata_store_pb2.INT
        preprocessing_dataset_type.properties["processed_courses_count"] = metadata_store_pb2.INT
        preprocessing_dataset_type.properties["null_responses_removed"] = metadata_store_pb2.INT
        preprocessing_dataset_type.properties["sensitive_data_flags"] = metadata_store_pb2.INT
        preprocessing_dataset_type.properties["timestamp"] = metadata_store_pb2.STRING
        preprocessing_dataset_type.properties["status"] = metadata_store_pb2.STRING
        preprocessing_dataset_type_id = store.put_artifact_type(preprocessing_dataset_type)

    # Create ExecutionType if it doesn't exist
    try:
        preprocessing_execution_type = store.get_execution_type("Preprocessing")
    except:
        preprocessing_execution_type = metadata_store_pb2.ExecutionType()
        preprocessing_execution_type.name = "Preprocessing"
        preprocessing_execution_type.properties["dag_run_id"] = metadata_store_pb2.STRING
        preprocessing_execution_type.properties["output_path"] = metadata_store_pb2.STRING
        preprocessing_execution_type.properties["start_time"] = metadata_store_pb2.STRING
        preprocessing_execution_type.properties["end_time"] = metadata_store_pb2.STRING
        preprocessing_execution_type_id = store.put_execution_type(preprocessing_execution_type)

    return store


def create_preprocessing_execution(store, **context):
    """
    Create an execution in MLMD for tracking the preprocessing run.
    """
    # Create execution
    execution = metadata_store_pb2.Execution()
    preprocessing_execution_type = store.get_execution_type("Preprocessing")
    execution.type_id = preprocessing_execution_type.id

    execution.properties["dag_run_id"].string_value = context['dag_run'].run_id
    execution.properties["output_path"].string_value = context['dag_run'].conf.get('output_path', '/tmp/processed_data')
    execution.properties["start_time"].string_value = datetime.utcnow().isoformat()
    
    execution_id = store.put_executions([execution])[0]
    return execution_id

def record_preprocessing_metadata(store, execution_id, metadata_values):
    """
    Record preprocessing metadata as artifacts in MLMD.
    """
    # Create artifact
    artifact = metadata_store_pb2.Artifact()
    preprocessing_artifact_type = store.get_artifact_type("PreprocessingDataset")
    artifact.type_id = preprocessing_artifact_type.id

    
    # Set properties
    artifact.properties["raw_reviews_count"].int_value = metadata_values["raw_reviews_count"]
    artifact.properties["raw_courses_count"].int_value = metadata_values["raw_courses_count"]
    artifact.properties["processed_reviews_count"].int_value = metadata_values["processed_reviews_count"]
    artifact.properties["processed_courses_count"].int_value = metadata_values["processed_courses_count"]
    artifact.properties["null_responses_removed"].int_value = metadata_values["null_responses_removed"]
    artifact.properties["sensitive_data_flags"].int_value = metadata_values["sensitive_data_flags"]
    artifact.properties["timestamp"].string_value = metadata_values["timestamp"]
    artifact.properties["status"].string_value = metadata_values["status"]
    
    # Put artifact in store
    artifact_id = store.put_artifacts([artifact])[0]
    
    # Create event to link execution and artifact
    event = metadata_store_pb2.Event()
    event.execution_id = execution_id
    event.artifact_id = artifact_id
    event.type = metadata_store_pb2.Event.Type.OUTPUT
    
    store.put_events([event])
    
    return artifact_id

def preprocess_data(**context):
    output_path = context['dag_run'].conf.get('output_path', '/tmp/processed_data')
    
    # Setup ML Metadata
    store = setup_mlmd()
    execution_id = create_preprocessing_execution(store, **context)
    
    # Initialize metadata values
    metadata_values = {
        "raw_reviews_count": 0,
        "raw_courses_count": 0,
        "processed_reviews_count": 0,
        "processed_courses_count": 0,
        "null_responses_removed": 0,
        "sensitive_data_flags": 0,
        "timestamp": datetime.utcnow().isoformat(),
        "status": "processing"
    }

    try:
        # Load data
        reviews_df = pd.read_csv(f"{output_path}/reviews.csv")
        courses_df = pd.read_csv(f"{output_path}/courses.csv")
        
        # Record initial counts
        metadata_values["raw_reviews_count"] = len(reviews_df)
        metadata_values["raw_courses_count"] = len(courses_df)
        
        # Initial metadata recording
        artifact_id = record_preprocessing_metadata(store, execution_id, metadata_values)

        # Preprocess data
        reviews_df["response"] = reviews_df["response"].apply(clean_text)
        courses_df["course_title"] = courses_df["course_title"].apply(clean_text)

        # Track null responses removed
        null_count = reviews_df["response"].isnull().sum()
        reviews_df = reviews_df[reviews_df["response"].notnull()]
        metadata_values["null_responses_removed"] = null_count

        # Check for sensitive data
        flag, sensitive_data_found = check_for_gender_bias(reviews_df, "response")
        sensitive_count = len(sensitive_data_found) if flag else 0
        metadata_values["sensitive_data_flags"] = sensitive_count

        if flag:
            logging.warning("Sensitive data found in responses")
            logging.warning(sensitive_data_found)

        # Record final counts
        metadata_values["processed_reviews_count"] = len(reviews_df)
        metadata_values["processed_courses_count"] = len(courses_df)

        # Save preprocessed data
        reviews_preprocessed_path = f"{output_path}/reviews_preprocessed.csv"
        courses_preprocessed_path = f"{output_path}/courses_preprocessed.csv"
        reviews_df.to_csv(reviews_preprocessed_path, index=False)
        courses_df.to_csv(courses_preprocessed_path, index=False)

        # Update metadata with success status
        metadata_values["status"] = "completed"
        artifact_id = record_preprocessing_metadata(store, execution_id, metadata_values)
        
        # Update execution end time
        execution = store.get_executions_by_id([execution_id])[0]
        execution.properties["end_time"].string_value = datetime.utcnow().isoformat()
        store.put_executions([execution])
        
        # Log metadata summary
        logging.info(f"Preprocessing metadata: {metadata_values}")
        
        return {
            'reviews_count': len(reviews_df),
            'courses_count': len(courses_df),
            'metadata': metadata_values,
            'execution_id': execution_id,
            'artifact_id': artifact_id
        }
        
    except Exception as e:
        # Update metadata with failure status
        metadata_values["status"] = "failed"
        record_preprocessing_metadata(store, execution_id, metadata_values)
        
        # Update execution end time
        execution = store.get_executions_by_id([execution_id])
        execution.properties["end_time"].string_value = datetime.utcnow().isoformat()
        store.put_executions([execution])
        
        logging.error(f"Error in preprocessing: {str(e)}")
        raise

def get_preprocessing_metadata(store, execution_id):
    """
    Utility function to retrieve preprocessing metadata for a given execution.
    """
    execution = store.get_executions_by_id([execution_id])[0]
    events = store.get_events_by_execution_ids([execution_id])
    
    if events:
        artifact_id = events[0].artifact_id
        artifact = store.get_artifacts_by_id([artifact_id])[0]
        
        metadata = {
            "raw_reviews_count": artifact.properties["raw_reviews_count"].int_value,
            "raw_courses_count": artifact.properties["raw_courses_count"].int_value,
            "processed_reviews_count": artifact.properties["processed_reviews_count"].int_value,
            "processed_courses_count": artifact.properties["processed_courses_count"].int_value,
            "null_responses_removed": artifact.properties["null_responses_removed"].int_value,
            "sensitive_data_flags": artifact.properties["sensitive_data_flags"].int_value,
            "status": artifact.properties["status"].string_value,
            "timestamp": artifact.properties["timestamp"].string_value,
            "dag_run_id": execution.properties["dag_run_id"].string_value,
            "start_time": execution.properties["start_time"].string_value,
            "end_time": execution.properties.get("end_time", metadata_store_pb2.Value()).string_value
        }
        
        return metadata
    return None
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
                sensitive_data_found.loc[-1] = [row["crn"], row["question"], row[column_name]]

                df.at[index, column_name] = row[column_name].replace(pron, "the professor")
    return flag, sensitive_data_found



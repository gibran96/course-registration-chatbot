import pandas as pd
import logging
from google.cloud import storage, bigquery
from airflow.models import Variable
import fitz
import gc
import uuid
from datetime import datetime
from airflow.models import Variable
from airflow.operators.dagrun_operator import TriggerDagRunOperator


from scripts.data.data_anomalies import check_for_gender_bias
from scripts.data.data_utils import check_unknown_text, clean_response, clean_text
from scripts.email_triggers import send_anomaly_email
from scripts.mlmd.mlmd_preprocessing import create_preprocessing_execution, record_preprocessing_metadata, setup_mlmd



# Question mapping
question_map = {
    "What were the strengths of this course and/or this instructor?": "Q1",
    "What could the instructor do to make this course better?": "Q2",
    "Please expand on the instructor's strengths and/or areas for improvement in facilitating inclusive learning.": "Q3",
    "Please comment on your experience of the online course environment in the open-ended text box.": "Q4",
    "What I could have done to make this course better for myself.": "Q5"
}

def extract_data_from_pdf(pdf_file):
    """
    Extracts structured data from a PDF file containing course information and responses.
    Args:
        pdf_file (list): A list of PDF pages, where each page is an object that has a `get_text` method.
    Returns:
        dict: A dictionary containing the extracted data with the following keys:
            - "crn" (str): Course Registration Number.
            - "course_title" (str): Title of the course.
            - "course_code" (str): Code of the course.
            - "instructor" (str): Name of the instructor.
            - "term" (str): Term during which the course is offered.
            - "responses" (list): A list of dictionaries, each containing:
                - "question" (str): The question text.
                - "responses" (list): A list of responses to the question.
    """
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

def parse_data(structured_data, reviews_df, courses_df):
    """
    Parses structured data and updates the reviews and courses dataframes.
    Args:
        structured_data (dict): A dictionary containing course and review information.
            Expected keys are:
                - "crn" (str): Course Reference Number.
                - "course_code" (str): Code of the course.
                - "course_title" (str): Title of the course.
                - "instructor" (str): Name of the instructor.
                - "term" (str): Term in which the course is offered.
                - "responses" (list): A list of dictionaries, each containing:
                    - "question" (str): The question asked.
                    - "responses" (list): A list of responses to the question.
        reviews_df (pandas.DataFrame): DataFrame containing existing reviews data.
        courses_df (pandas.DataFrame): DataFrame containing existing courses data.
    Returns:
        tuple: A tuple containing updated reviews_df and courses_df.
    """
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
                    "response": response
                }
                reviews_df = reviews_df._append(new_review_row, ignore_index=True)
                
    return reviews_df, courses_df

def read_and_parse_pdf_files(**context):
    """
    Reads and parses PDF files from a specified Google Cloud Storage bucket, extracts data, and saves it to CSV files.
    Args:
        **context: A dictionary containing context information passed from the DAG run. Expected keys include:
            - 'dag_run': The DAG run object, which should contain 'conf' with 'bucket_name' and 'output_path'.
            - 'ti': The task instance object, used to pull XCom data.
    Returns:
        dict: A dictionary containing the counts of reviews and courses processed, with keys:
            - 'reviews_count': The number of reviews processed.
            - 'courses_count': The number of courses processed.
    Raises:
        Exception: If there is an error during the processing of PDFs.
    The function performs the following steps:
        1. Initializes empty DataFrames for reviews and courses.
        2. Connects to the specified Google Cloud Storage bucket.
        3. Iterates over the blobs in the bucket, processing only PDF files that match the unique blobs.
        4. Downloads and processes each PDF file, extracting structured data.
        5. Parses the extracted data and appends it to the DataFrames.
        6. Logs the length of the DataFrames.
        7. Saves the processed data to CSV files, removing any existing files with the same name.
        8. Creates and saves a question mapping to a CSV file.
    """
    bucket_name = context['dag_run'].conf.get('bucket_name', Variable.get('default_bucket_name'))
    output_path = context['dag_run'].conf.get('output_path', '/tmp/processed_data')
    unique_blobs = context['ti'].xcom_pull(task_ids='get_unique_blobs', key='unique_blobs')
    
    # Initialize DataFrames
    reviews_df = pd.DataFrame(columns=["review_id", "crn", "question", "response"])
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
                reviews_df, courses_df = parse_data(structured_data, reviews_df, courses_df)
                
                pdf_file.close()
                gc.collect()
                
                logging.info(f"Processed {blob.name}")
        
        # print length of the dataframes
        logging.info(f"Length of reviews: {reviews_df.shape[0]}")
        logging.info(f"Length of courses: {courses_df.shape[0]}")

        # Push the data frames to xcom
        context['ti'].xcom_push(key='reviews_df', value=reviews_df)
        context['ti'].xcom_push(key='courses_df', value=courses_df)

        
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

def preprocess_data(**context):
    """
    Preprocesses review and course data, records metadata, and handles errors.
    Args:
        context (dict): Context information, including 'dag_run' configuration.
    Returns:
        dict: Contains 'reviews_count', 'courses_count', 'metadata', 'execution_id', 'artifact_id'.
    Raises:
        Exception: Logs error, updates metadata with failure status, and raises the exception.
    Metadata Values:
        - raw_reviews_count (int): Initial count of raw reviews.
        - raw_courses_count (int): Initial count of raw courses.
        - processed_reviews_count (int): Final count of processed reviews.
        - processed_courses_count (int): Final count of processed courses.
        - null_responses_removed (int): Count of null responses removed.
        - sensitive_data_flags (int): Count of sensitive data flags found.
        - timestamp (str): Timestamp of the execution.
        - status (str): Status of the execution ('processing', 'completed', 'failed').
    Steps:
        1. Setup ML Metadata store and create execution.
        2. Load data from CSV files.
        3. Record initial counts.
        4. Remove null responses and record the count.
        5. Preprocess text data.
        6. Check for sensitive data and record the count.
        7. Save preprocessed data to CSV files.
        8. Update metadata with success status and final counts.
        9. Update execution end time and log summary.
        10. Handle errors by updating metadata with failure status and logging the error.
    """
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
        # Load data from xcom 
        reviews_df = context['ti'].xcom_pull(task_ids='process_pdfs', key='reviews_df')
        courses_df = context['ti'].xcom_pull(task_ids='process_pdfs', key='courses_df')
        
        # Record initial counts
        metadata_values["raw_reviews_count"] = len(reviews_df)
        metadata_values["raw_courses_count"] = len(courses_df)
        
        # Initial metadata recording
        artifact_id = record_preprocessing_metadata(store, execution_id, metadata_values)

        # Track null responses removed
        null_count = reviews_df["response"].isnull().sum()
        reviews_df = reviews_df[reviews_df["response"].notnull()]
        metadata_values["null_responses_removed"] = null_count


        # Preprocess data
        reviews_df["response"] = reviews_df["response"].apply(clean_text)
        courses_df["course_title"] = courses_df["course_title"].apply(clean_text)


        # Check for sensitive data
        flag, sensitive_data_found = check_for_gender_bias(reviews_df, "response")
        sensitive_count = len(sensitive_data_found) if flag else 0
        metadata_values["sensitive_data_flags"] = sensitive_count

        if flag:
            logging.warning("Sensitive data found in responses")
            logging.warning(len(sensitive_data_found))

        anomaly_review_df, anomaly_course_df = check_unknown_text(reviews_df, courses_df)

        if not anomaly_review_df.empty:
            logging.warning(f"Unknown text found in reviews: {anomaly_review_df}")
            send_anomaly_email(f"Unknown text found in reviews: {anomaly_review_df}")
        if not anomaly_course_df.empty:
            logging.warning(f"Unknown text found in courses: {anomaly_course_df}")
            send_anomaly_email(f"Unknown text found in courses: {anomaly_course_df}")

        # Record final counts
        metadata_values["processed_reviews_count"] = len(reviews_df)
        metadata_values["processed_courses_count"] = len(courses_df)

        # Save preprocessed data
        reviews_preprocessed_path = f"{output_path}/reviews_preprocessed.csv"
        courses_preprocessed_path = f"{output_path}/courses_preprocessed.csv"
        reviews_df.astype(str).to_csv(reviews_preprocessed_path, index=False)
        courses_df.astype(str).to_csv(courses_preprocessed_path, index=False)
        

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

def get_crn_list(**context):
    """
    Retrieves a distinct list of CRNs (Course Reference Numbers) from a specified BigQuery table
    and pushes the list to XCom for further use in the Airflow DAG.

    Args:
        **context: Airflow context dictionary containing task instance (ti) and other metadata.

    Returns:
        list: A list of distinct CRNs retrieved from the BigQuery table. Returns an empty list if an error occurs.

    Raises:
        Exception: If there is an error during the BigQuery client query execution.
    """
    try :
        client = bigquery.Client()
        query = f"""
            SELECT DISTINCT crn
            FROM {Variable.get('review_table_name')}
            ORDER BY crn
            LIMIT 1000
        """
        logging.info(f"Executing query: {query}")
        crn_list = list(set(row["crn"] for row in client.query(query).result()))
        logging.info(f"CRN List: {len(crn_list)}")
        client.close()
        context['ti'].xcom_push(key='crn_list', value=crn_list)
        return crn_list
    except Exception as e:
        logging.error(f"Error: {e}")
        return []

def monitor_new_rows_and_trigger(**context):
    """
    Checks the number of new rows added to the table and triggers the next DAG if the new rows 
    exceed 10% of the existing rows.

    Args:
        context (dict): The context dictionary provided by Airflow, containing task instance 
                        and other metadata.

    The function performs the following steps:
    1. Retrieves the number of new rows added from the XCom of the 'get_unique_blobs' task.
    2. Logs the number of new rows added.
    3. Retrieves the number of rows already in the table from the XCom of the 'get_crn_list' task.
    4. Logs the number of rows already in the table.
    5. Checks if the number of new rows added is more than 10% of the existing rows.
    6. If the condition is met, triggers the 'train_data_dag' DAG.
    7. Logs whether the 'train_data_dag' was triggered or not.
    """
    new_rows = context['ti'].xcom_pull(task_ids='get_unique_blobs', key='unique_blobs')
    logging.info(f"Number of new rows added: {len(new_rows)}")

    # Number of rows already in the table
    crn_list = context['ti'].xcom_pull(task_ids='get_crn_list', key='crn_list')
    logging.info(f"Number of rows already in the table: {len(crn_list)}")

    # Check if the new rows added are more than 10% of the existing rows
    if len(new_rows) > 0.1 * len(crn_list):
        # Trigger the next DAG
        trigger_train_data_pipeline = TriggerDagRunOperator(
            task_id='trigger_train_data_pipeline',
            trigger_dag_id='train_data_dag'
        )
        logging.info("Triggering the train_data_dag")
        trigger_train_data_pipeline.execute(context=context)
    else:
        logging.info("Not triggering the train_data_dag")

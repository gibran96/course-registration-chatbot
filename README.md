# Course Registration Chatbot

## Overview

This project aims to develop an LLM-powered chatbot to streamline and simplify the course registration process for college students. The chatbot will integrate data from multiple sources, including Banner and Trace, to provide personalised course recommendations, verify program requirements, check prerequisites, and offer insights on professor reviews. The system will be built using Google Cloud
Platform (GCP) and will incorporate MLOps best practices, including monitoring and CI/CD pipelines, to ensure robust and efficient operation.

This repository hosts the data pipeline designed to collect, process, and store data related to Northeastern University course offerings and student feedback.


## Data Pipeline - Key Components and Workflow

### 1. Data Collection
The pipeline collects three main types of data:
- **Course Information**: Web scraping scripts fetch details about course offerings from the NEU Banner system, including CRN, title, instructor, etc.
- **Student Feedback**: Extracts student feedback from NEU Trace instructor comments reports, which are available as PDF documents and uploaded to Google Cloud Storage (GCS) for processing.
- **Training Data**: Uses specific seed queries and refined data from BigQuery to generate structured data for training purposes.

### 2. Data Processing
The collected data undergoes several stages of processing:
- **PDF Data Extraction**: Python-based scripts analyze the PDF files to extract relevant course evaluation comments.
- **Data Cleaning**: Removes irrelevant details and standardizes the data to ensure consistency across different sources.
- **Structured Data Creation**: Organizes the cleaned data into structured datasets optimized for analysis.

### 3. Train Data Generation
Using the `train_data_dag` in Airflow, the pipeline generates seed data for training a model. This process involves:
- **Data Retrieval**: Fetches initial data from BigQuery and performs similarity searches to refine the dataset.
- **LLM Response Generation**: Generates responses from a language model (LLM) based on the seed queries and processed data.
- **Data Upload**: The generated data is uploaded to GCS and then loaded back into BigQuery to be used as training data.

This automated DAG enables systematic preparation of data needed for training and ensures consistency in the generation process.

### 4. Data Storage
The processed data is stored for easy access and analysis:
- **Google Cloud Storage (GCS)**: Intermediate results, final datasets, and training data are stored in designated GCS buckets.
- **BigQuery**: Structured datasets and training data are ingested into BigQuery tables for efficient querying and analysis.

## Airflow DAGs Overview

### 1. **Banner Data DAG Pipeline** (`banner_dag_pipeline`)
This Airflow DAG focuses on fetching data from the NEU Banner system, processing it, and storing it in BigQuery. The steps are as follows:

![Banner Data DAG](./assets/imgs/Banner_Data_DAG.png)


- **get_cookies_task**: Retrieves authentication cookies to access Banner data.
- **get_course_list_task**: Obtains a list of courses from the Banner system.
- **get_faculty_info_parallel**: Collects faculty information for each course, running tasks in parallel.
- **get_course_description_parallel**: Fetches course descriptions in parallel for efficiency.
- **dump_to_csv_task**: Converts the data into a CSV format for easier handling and debugging.
- **upload_to_gcs**: Uploads the CSV file to Google Cloud Storage.
- **load_banner_data_to_bq**: Loads the Banner data from GCS into BigQuery.
- **Success Email**: Sends an email notification upon successful data loading.

![image](https://github.com/user-attachments/assets/21b726d8-d294-44a5-a39b-d9ec28b4a64d)


This pipeline streamlines the process of gathering and storing course and faculty information from the Banner system. 

### 2. **PDF Processing Pipeline** (`pdf_processing_pipeline`)
This Airflow DAG is set up to process NEU Trace course review data. It fetches data, processes it, and then stores it in a BigQuery table. Here’s how it works:

![PDF Processing DAG](./assets/imgs/PDF_Processing_DAG.png)

- **select_distinct_crn**: Selects unique Course Registration Numbers (CRNs) to identify distinct courses.
- **get_crn_list**: Fetches a list of CRNs to be processed.
- **get_unique_blobs**: Retrieves unique PDF files from the data source.
- **process_pdfs**: Extracts data from each PDF.
- **preprocess_pdfs**: Prepares the extracted data for storage by cleaning and structuring it.
- **upload_to_gcs**: Uploads the preprocessed data to Google Cloud Storage.
- **load_courses_to_bigquery** and **load_to_bigquery**: Loads processed course and review data into specific BigQuery tables.
- **Success Email**: Notifies the team upon successful completion of the data processing.
- **trigger_train_data_pipeline**: Triggers the training data pipeline once PDF processing is complete.

![image](https://github.com/user-attachments/assets/77d2511b-a7e0-42c0-9bfc-fe2e3d6d0d94)


This pipeline is essential for organizing and storing course review data in a structured format for analysis.


### 3. **Train Data DAG** (`train_data_dag`)

![Train Data DAG](./assets/imgs/Train_Data_DAG.png)

This Airflow Directed Acyclic Graph (DAG) is responsible for generating seed queries to train a model. It involves several steps:

- **check_sample_count**: Ensures that there are enough samples available for training.
- **get_bq_data**: Retrieves data from BigQuery to be used for training.
- **get_initial_queries**: Generates initial queries based on the data retrieved.
- **bq_similarity_search**: Uses BigQuery to perform similarity searches, which help in refining the data for training.
- **generate_llm_response**: Generates responses from a language model based on the seed data.
- **upload_to_gcs**: Uploads processed data to Google Cloud Storage.
- **upload_gcs_to_bq**: Loads the data from Google Cloud Storage back into BigQuery.
- **trigger_dag_run**: Triggers additional DAG runs if necessary.
- **success_email**: Sends an email notification upon successful completion of the pipeline.

![image](https://github.com/user-attachments/assets/9b59415b-47c9-4ed2-a50d-bf5b00c40fef)


This DAG is designed to automate the preparation and processing of training data in a systematic way.

These DAGs automate and organize different stages of the data pipeline, each targeting a specific dataset (training data, course reviews, or Banner data) for Northeastern University courses.

## Project Directory Structure and Description
```
├── .github
│   └── workflows
│       ├── gcd-upload.yaml: This workflow defines a CI/CD pipeline to upload the data pipeline code to Google Cloud Storage. 
│            It triggers on pushes to the main and workflow-testing branches and uses gsutil to copy the files.
│       └── python-tests.yaml: This workflow runs Python tests using pytest. It is triggered by pushes to the main, 
│            fix-testcases-import-error, and gibran/banner-dag branches and sets up a Python environment with the 
│            necessary dependencies.
│
├── .gitignore: This file specifies files and directories that should be ignored by Git. It includes common Python artifacts, 
│    build directories, test results, and various IDE configuration files.
│
├── README.md: This file provides documentation for the project, including an overview, architecture diagrams, and instructions 
│    for contributing. It explains the purpose of the project and its different components.
│
├── assets
│   └── imgs: This directory contains images used in the README.md file, including visualizations of the Airflow DAGs. 
│        These images help illustrate the workflow of the data pipeline.
│
├── data_pipeline
│   ├── __init__.py: This file initializes the data_pipeline package. It makes the package importable and can be used 
│        to define any package-level initialization logic.
│
│   ├── dags
│       ├── __init__.py: This file initializes the dags package within the data pipeline. Similar to other __init__.py files, 
│            it makes the dags directory a package.
│       ├── banner_data_dag.py: This file defines the Airflow DAG for fetching course data from the NEU Banner system. 
│            It scrapes course details, faculty information, and prerequisites, then uploads the data to GCS and BigQuery.
│       ├── main.py: This file defines the Airflow DAG that processes PDF files from GCS. It extracts, preprocesses, 
│            and loads course review data into BigQuery, and subsequently triggers the training data pipeline.
│
│       ├── scripts
│           ├── __init__.py: This file initializes the scripts package within the dags directory. It allows for modularization 
│                and easier management of scripts used by the DAGs.
│           ├── backoff.py: This file defines a decorator for implementing exponential backoff retry logic. It's used to handle 
│                transient errors during API calls or other operations.
│           ├── bigquery_utils.py: This file contains utility functions for interacting with BigQuery, such as checking sample counts 
│                and retrieving data. It handles tasks such as data extraction, similarity search, and data loading.
│           ├── constants.py: This file defines constants used throughout the data pipeline scripts. It centralizes configuration parameters 
│                like project ID, sample counts, and LLM prompts.
│           ├── data_processing.py: This file contains functions for processing data, specifically for generating initial queries for the LLM. 
│                It prepares data before it is used in other parts of the pipeline.
│           ├── data_utils.py: This file defines utility functions for data manipulation, such as uploading data to GCS and removing punctuation. 
│                These functions are reused across multiple DAGs.
│           ├── extract_data.py: This file contains functions for extracting course and review data. It processes files, parses data, 
│                and updates dataframes for processing and storage.
│           ├── extract_trace_data.py: This file contains functions for extracting, preprocessing, and validating the extracted TRACE instructor 
│                comments data from PDF files. This includes handling null responses, checking for sensitive information, and metadata recording.
│           ├── fetch_banner_data.py: This file defines functions for fetching course information from the NEU Banner system, including cookies, 
│                course lists, descriptions, and prerequisites. It interacts with the Banner API and parses the HTML responses.
│           ├── llm_utils.py: This file provides functions for interacting with the Large Language Model (LLM). It handles prompt generation, 
│                response retrieval, parsing, and generation of training data.
│           ├── opt_fetch_banner_data.py: This file optimizes the fetching of Banner data by implementing parallel processing. 
│                It uses multithreading to speed up the data retrieval process.
│           └── seed_data.py: This file contains seed data, including topic lists and query templates. It provides initial input 
│                and guides the LLM in generating relevant responses.
│
│       ├── tests
│           ├── __init__.py: This file initializes the tests package within the dags directory. It allows for running tests 
│                specifically for the DAG scripts.
│           ├── test_extract_data.py: This file contains unit tests for functions defined in extract_trace_data.py. 
│                It helps ensure the correctness of the data extraction process.
│           └── test_fetch_banner_data.py: This file contains unit tests for the Banner data fetching functions. 
│                It validates the behavior of the API interaction and data parsing logic.
│
│       └── train_data_dag.py: This file defines the Airflow DAG for generating synthetic training data. 
│            It retrieves data from BigQuery, generates queries using an LLM, performs similarity searches, and uploads 
│            the results to GCS and BigQuery.
│
│   ├── data
│       └── dag_3.py: This file contains another Airflow DAG (course_review_llm_pipeline). This appears to be a deprecated file 
│            or earlier version of the main processing DAG or train data DAG since its functionality appears to be redundant.
│
│   └── logs
│       └── __init__.py: This file initializes the logs package within the data pipeline. It may contain utilities for logging 
│            or is simply a placeholder for future logging functionality.
│
└── requirements.txt: This file lists the project dependencies required to run the data pipelines. It's used by pip 
     to install the necessary packages.
```
## Tools and Technologies
- **Python**: Core programming language for development.
- **Google Cloud Platform (GCP)**: Provides cloud infrastructure for storage and computation.
- **Cloud Composer**: Managed workflow orchestration tool on Google Cloud that uses Apache Airflow to automate, schedule, and monitor complex data pipelines.
- **Google Cloud Storage:** Cloud-based storage solution used to store, manage, and retrieve large volumes of structured and unstructured data, making it ideal for data warehousing and big data analytics tasks.
- **BigQuery**: Used for storing and analyzing large datasets.


## Contributing
We welcome contributions to improve the data pipeline. If you wish to contribute:
- Fork the repository.
- Make changes to the codebase.
- Submit a pull request detailing your modifications.

## License
Distributed under the MIT License. See `LICENSE.txt` for more information.

## Contact
For any inquiries or issues regarding the data pipeline, please reach out to one of the repository owners:

- **Gibran Myageri** - myageri.g@northeastern.edu
- **Goutham Yadavall** - yadavalli.s@northeastern.edu
- **Kishore Sampath** - kishore.sampath@neu.edu
- **Rushikesh Ghatage** - ghatage.r@northeastern.edu
- **Mihir Athale** - athale.m@northeastern.edu

# Course Registration Chatbot - Data Pipeline

## Overview
This repository hosts the data pipeline designed to collect, process, and store data related to Northeastern University course offerings and student feedback.

The primary objective of the project is to generate insights useful for prospective students when selecting courses to register for.

## Key Components

### 1. Data Collection

The pipeline collects two types of data:
- **Course Information**: Scrapes course offering details from the NEU Banner system.
- **Student Feedback**: Extracts student feedback from NEU Trace instructor comments reports available as PDF documents.

### 2. Data Processing
The collected data undergoes several stages of processing:
- **PDF Data Extraction**: Uses Python-based extraction techniques to obtain student feedback from the NEU Trace instructor comments PDF reports.
- **Data Cleaning**: Standardizes the data by removing irrelevant details and ensuring consistency across sources.
- **Structured Data Creation**: Formats the data into well-structured datasets optimized for analysis.

### 3. Storage
The processed data is stored in Google Cloud Storage (GCS) buckets and BigQuery databases, enabling easy access and analysis.

## Main Workflow

1. **Data Ingestion**:
   - **Course Data**: Web scraping scripts fetch information about available courses including CRN, title, instructor, etc.
   - **Feedback Data**: PDF files containing student feedback are uploaded to GCS.

2. **Data Processing**:
   - **PDF Data Processing**: Scripts analyze PDFs to extract relevant course evaluation comments.
   - **Data Cleaning and Structuring**: Processed data is cleaned, formatted, and organized into structured datasets.

3. **Data Storage**:
   - **GCS**: Intermediate results and final datasets are stored in designated GCS buckets.
   - **BigQuery**: Processed datasets are ingested into BigQuery tables for querying and analysis.

## Tools and Technologies
- **Python**: Core programming language for development.
- **Google Cloud Platform (GCP)**: Provides cloud infrastructure for storage and computation.
- **Cloud Composer**: Managed workflow orchestration tool on Google Cloud that uses Apache Airflow to automate, schedule, and monitor complex data pipelines.
- **Google Cloud Storage:** Cloud-based storage solution used to store, manage, and retrieve large volumes of structured and unstructured data, making it ideal for data warehousing and big data analytics tasks.
- **BigQuery**: Used for storing and analyzing large datasets.

## Getting Started
To set up and run the data pipeline locally:
1. Clone the repository to your local machine.
2. Configure necessary environment variables such as GCP service account keys and database connection details.
3. Run the Airflow DAGs to execute the data collection, processing, and storage tasks.

## Contributing
We welcome contributions to improve the data pipeline. If you wish to contribute:
- Fork the repository.
- Make changes to the codebase.
- Submit a pull request detailing your modifications.

## License
This project is licensed under the MIT License. See LICENSE for details.

## Contact
For any inquiries or issues regarding the data pipeline, please reach out to one of the repository owners:

- **Gibran** - owner1@neu.edu
- **Goutham** - owner2@neu.edu
- **Kishore Sampath** - kishore.sampath@neu.edu
- **Mihir** - owner4@neu.edu
- **Rishi** - owner5@neu.edu

---
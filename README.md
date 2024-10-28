# Course Registration Chatbot Data Pipeline

## Overview
This repository hosts a comprehensive data pipeline designed to collect, process, and store data related to university course offerings and student feedback. The primary objective is to generate insights useful for prospective students when selecting courses to register for.

## Key Components

### 1. Data Collection
The pipeline collects two types of data:
- **Course Information**: Scraped from university systems via web scraping techniques.
- **Student Feedback**: Extracted from PDF documents containing detailed course evaluations.

### 2. Data Processing
The collected data undergoes several stages of processing:
- **PDF Data Extraction**: Utilizes Optical Character Recognition (OCR) tools to convert PDF content into machine-readable format.
- **Data Cleaning**: Removes irrelevant information and formats data consistently across different sources.
- **Structured Data Creation**: Organizes extracted data into well-structured datasets suitable for analysis.

### 3. Storage
Processed data is stored in Google Cloud Storage (GCS) buckets and BigQuery databases for easy access and further analysis.

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
- **Airflow**: Orchestration tool managing the data pipeline workflow.
- **Google Cloud Platform (GCP)**: Provides cloud infrastructure for storage and computation.
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
For any inquiries or issues regarding the data pipeline, please contact us at <EMAIL>.

---

This markdown format provides a clear and concise overview of the course registration chatbot data pipeline project, detailing its purpose, components, workflow, technologies used, and instructions for getting started and contributing to the project. It adheres to best practices for README files, making it easy for others to understand and use the data pipeline effectively.
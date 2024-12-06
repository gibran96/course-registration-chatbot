## Data Drift Detection Pipeline

### Table of Contents
1. [Introduction](#introduction)
2. [Architecture](#architecture)
3. [Directory Structure](#directory-structure)
4. [Installation](#installation)
5. [Configuration](#configuration)
6. [Usage](#usage)
7. [Pipeline Steps](#pipeline-steps)
8. [Dependencies](#dependencies)
9. [Logging and Monitoring](#logging-and-monitoring)
10. [Troubleshooting](#troubleshooting)
11. [Contributing](#contributing)
12. [License](#license)
13. [Contact](#contact)

---

### Introduction

The **Data Drift Detection Pipeline** is designed to monitor and detect changes in data distributions over time. Data drift can significantly impact the performance of machine learning models, leading to inaccurate predictions and degraded user experiences. This pipeline automates the process of detecting such drifts and triggers necessary actions, including model retraining workflows, to ensure models remain robust and reliable.

### Architecture

![Data Drift Detection Pipeline](https://github.com/user-attachments/assets/4a23de1b-ee54-4eb3-a824-316fee49f5ab)

The architecture comprises the following components:

- **Data Extraction**: Retrieves training and test questions from BigQuery.
- **Embedding Generation**: Generates embeddings for both training and test datasets using Vertex AI's embedding models.
- **Threshold Determination**: Calculates similarity thresholds to identify significant drifts.
- **Drift Detection**: Compares embeddings to detect drift based on predefined thresholds.
- **Action Triggering**: If drift is detected, triggers workflows such as data regeneration and model retraining.
- **Data Archival**: Moves processed data to archival storage for historical reference.
- **Notification**: Sends email notifications upon successful execution or detection of drift.

### Directory Structure

```
data_drift/
├── README.md
├── __init__.py
└── dags/
    ├── __init__.py
    ├── data_drift_detection_dag.py
    └── scripts/
        ├── __init__.py
        ├── backoff.py
        ├── bigquery_utils_data_drift.py
        ├── constants_data_drift.py
        ├── data_regeneration.py
        ├── drift_detection.py
        ├── gcs_utils_data_drift.py
        └── llm_utils_data_drift.py
```

- **README.md**: Documentation outlining the pipeline, its components, and usage.
- **dags/**: Contains Airflow DAGs orchestrating the pipeline tasks.
- **scripts/**: Utility scripts for various tasks like BigQuery interactions, drift detection logic, GCS operations, and LLM utilities.

### Installation

1. **Clone the Repository**
   ```bash
   git clone https://github.com/gibran96/course-registration-chatbot.git
   cd course-registration-chatbot/data_drift
   ```

2. **Set Up Python Environment**
   Ensure you have Python 3.8+ installed. Create a virtual environment and install dependencies:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Configure Airflow**
   - Place the `data_drift` directory in your Airflow `dags` folder.
   - Ensure Airflow has access to the necessary Google Cloud credentials.

### Configuration

Configure the pipeline by setting the required Airflow Variables:

- `train_data_table_name`: BigQuery table containing training questions.
- `user_data_table_name`: BigQuery table containing user queries.
- `historic_user_data_table_name`: BigQuery table for archival of user data.
- `default_bucket_name`: Google Cloud Storage bucket for data uploads.
- `data_drift_table_name`: BigQuery table to log drift history.
- `drift_last_detected_at`: Timestamp of the last detected drift.

These can be set via the Airflow UI under **Admin > Variables** or using the Airflow CLI.

### Usage

1. **Trigger the DAG**
   - Navigate to the Airflow UI.
   - Locate the `data_drift_detection_dag`.
   - Trigger the DAG manually or set it to run on a schedule.

2. **Monitor Execution**
   - Monitor task statuses and logs via the Airflow UI.
   - Check for email notifications upon successful runs or drift detections.

### Pipeline Steps

1. **Get Training Questions (`get_train_questions`)**
   - Fetches training questions from BigQuery.

2. **Get Test Questions (`get_test_questions`)**
   - Retrieves new user queries from BigQuery.

3. **Generate Train Embeddings (`get_train_embeddings`)**
   - Generates embeddings for training questions using Vertex AI.

4. **Generate Test Embeddings (`get_test_embeddings`)**
   - Generates embeddings for test questions.

5. **Determine Thresholds (`get_thresholds`)**
   - Calculates similarity thresholds for drift detection.

6. **Detect Data Drift (`data_drift_detection`)**
   - Compares embeddings to identify data drift.

7. **Data Drift Trend Task (`data_drift_trend_task`)**
   - Analyzes drift trends over time.

8. **Similarity Search Results (`bq_similarity_search`)**
   - Performs similarity searches based on drift queries.

9. **Generate LLM Response (`generate_llm_response`)**
   - Uses LLMs to generate responses for drift queries.

10. **Upload Train Data to GCS (`upload_train_data_to_gcs`)**
    - Uploads regenerated training data to GCS.

11. **Upload GCS to BigQuery (`upload_gcs_to_bq`)**
    - Loads data from GCS into BigQuery.

12. **Trigger DAG Run (`trigger_dag_run`)**
    - Triggers the `train_model_trigger_dag` for model retraining.

13. **Move Data from User Table (`move_data_from_user_table`)**
    - Archives processed user data.

14. **Send Success Email (`success_email`)**
    - Sends an email notification upon successful execution.

### Dependencies

- **Airflow**: Orchestrates the DAGs.
- **Google Cloud SDK**: For BigQuery and GCS interactions.
- **Vertex AI**: For embedding generation and LLM utilities.
- **Python Libraries**:
  - `google-cloud-bigquery`
  - `google-cloud-storage`
  - `pandas`
  - `numpy`
  - `scikit-learn`
  - `vertexai`
  - `logging`
  - Others as specified in `requirements.txt`.

### Logging and Monitoring

- **Airflow Logs**: Accessible via the Airflow UI for each task.
- **Email Notifications**: Configured to send emails on DAG success or failure.
- **BigQuery Logs**: Drift history is stored for historical analysis.

### Troubleshooting

- **Common Issues**:
  - **Authentication Errors**: Ensure Airflow has the correct Google Cloud credentials.
  - **Missing Variables**: Verify all required Airflow Variables are set.
  - **Resource Limits**: Check Vertex AI quotas and limits.

- **Debugging Steps**:
  - Inspect Airflow task logs for error messages.
  - Validate BigQuery queries separately.
  - Ensure GCS bucket permissions are correctly configured.
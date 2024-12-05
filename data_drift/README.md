# Detecting Data Drift

This DAG is designed to detect data drift and trigger further processes, including retraining workflows, if drift is identified. Below is a step-by-step outline of the tasks performed:


## Data Drift Pipeline Directory Structure and Description

```
| data_drift/
| ├── dags/
| │   ├── data_drift_detection_dag.py: The main DAG that orchestrates the process of data drift detection and triggers further workflows.
| ├── scripts/
| │   ├── __init__.py: Initializes the scripts package for utility functions related to data drift detection.
| │   ├── backoff.py: Implements an exponential backoff strategy for retrying failed operations during data drift detection.
| │   ├── bigquery_utils_data_drift.py: Contains BigQuery utilities tailored to the data drift detection process.
| │   ├── constants_data_drift.py: Defines constants used across data drift detection tasks.
| │   ├── data_regeneration.py: Contains logic for regenerating data when data drift is detected.
| │   ├── drift_detection.py: Core logic for comparing embeddings and detecting data drift.
| │   ├── gcs_utils_data_drift.py: Utility functions for interacting with Google Cloud Storage during the data drift detection process.
| │   ├── llm_utils_data_drift.py: Functions for utilizing large language models (LLMs) in data drift detection.
| ├── __init__.py: Initializes the data_drift package.
| └── README.md: Documentation for the data_drift module, outlining the DAG, steps involved, and implementation details.
```

## Data Drift Detection Dag Pipeline
![image](https://github.com/user-attachments/assets/970f25e8-c267-410d-aebe-96b11be94ee1)


## Steps to Drift Detection

### 1. **Get Training Questions**
   - **Task:** `get_train_questions`
   - **Description:** Fetches the questions used to train the model from BigQuery to be used for drift detection.

### 2. **Get Test Questions**
   - **Task:** `get_test_questions`
   - **Description:** Retrieves new questions asked by users from BigQuery for comparison with training data.

### 3. **Generate Train Embeddings**
   - **Task:** `get_train_embeddings`
   - **Description:** Generates embeddings for the training questions for use in similarity and drift detection.

### 4. **Generate Test Embeddings**
   - **Task:** `get_test_embeddings`
   - **Description:** Creates embeddings for the test questions to compare against the training data.

### 5. **Determine Thresholds**
   - **Task:** `get_thresholds`
   - **Description:** Calculates thresholds for detecting significant drift in the data by taking the minimum cosine score amongst the training questions, we take 0.9*min_score as upper threshold and 0.6*min_score as lower threshold.

### 6. **Detect Data Drift**
   - **Task:** `data_drift_detection`
   - **Description:** Compares train and test embeddings to determine whether data drift has occurred. If the minimum similarity is between lower and upper threshold then drift is detected and we re-generate the training data and re-trigger training.

### 7. **Regenerate Training Data**
   - **Description:** If a drift is detected we follow a similar approach to train data generation to generate training data for the new user queries by performing RAG over our database in BigQuery. We load this data in the training dataset and re-train the model to make it learn new user queries.

### 11. **Trigger Retraining Workflow**
   - **Task:** `trigger_dag_run`
   - **Description:** If data drift is detected, this task triggers a retraining DAG (`train_data_dag`) to update models with the latest data.

### 12. **Move Data from User Table**
   - **Task:** `move_data_from_user_table`
   - **Description:** Moves processed data from user-specific tables into archival table.

### 13. **Send Success Email**
   - **Task:** `success_email`
   - **Description:** Sends an email notification confirming the successful execution of the DAG.

---

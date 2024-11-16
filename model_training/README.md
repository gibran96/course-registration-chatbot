# Model Pipeline - Key Components and Workflow

## Overview

The pipeline automates the process of fine-tuning, evaluating, and deploying a large language model (LLM) for generating course-related responses.

## Key Components

1. **Data Preparation**
The data preparation process leverages Apache Airflow, Google BigQuery, and Google Cloud Storage (GCS) to create a robust and automated workflow:
   - **End-to-End Automation**: The pipeline detects new data in BigQuery and triggers the data preparation process without manual intervention.
   - **Data Retrieval**: An Airflow DAG queries BigQuery using predefined SQL to fetch fields like query, context, and response from relevant tables.
   - **Data Cleaning and Transformation**: The pipeline removes null or invalid entries, handles missing data, and standardizes text fields for model readiness.
   - **Data Formatting**: Converts data into JSONL format for compatibility with fine-tuning workflows, ensuring fields are aligned for supervised fine-tuning and evaluation.

2. **Model Training**
The model training process utilizes the gemini-1.5-flash-002 base model and implements supervised fine-tuning (SFT) on task-specific data:
   - **Fine-Tuning Process:** Integrates with Vertex AI for scalable training and experiment tracking.
   - **Version Control:** Implements version control for fine-tuned models in Vertex AI, allowing for easy rollback and performance tracking across different versions.

3. **Model Evaluation**
The evaluation system employs both standard and custom metrics to ensure comprehensive model assessment:
   - **Standard Metrics:** BLEU, ROUGE (1, 2, L).
   - **Custom Metrics:** Relevance (degree to which the response addresses the query), Coverage (completeness and depth of the response)
   - **Bias Detection:** Implements a 5-point rubric to evaluate gender neutrality and inclusivity in responses

4. **Model Registry**
The pipeline utilizes Vertex AI for model versioning, metadata storage, and deployment:
   - **Version Control:** Every trained model is saved as a versioned endpoint in Vertex AI, including metadata such as hyperparameters and dataset details.
   - **Experiment Tracking:** Automatically logs training parameters, metrics, and artifacts for future reference and reproducibility.
   - **Deployment Integration:** Models are directly deployable from the registry to Vertex AI endpoints.

5. **Bias Detection and Mitigation**
The bias detection system ensures fairness and inclusivity in model responses:
   - **Query Generation:** Creates diverse prompts to test responses for bias, including questions about teaching style and approachability.
   - **Response Evaluation:** Uses sentiment analysis and manual scoring to rate responses on a 5-point rubric.
   - **Bias Report:** Aggregates results into a detailed report, highlighting patterns and areas for improvement.

6. **CICD Pipeline**
The Continuous Integration and Continuous Deployment (CI/CD) pipeline automates the entire process from data preparation to model deployment:
   - **Triggering:** Detects data updates in BigQuery, and in the code using GitHub actions and automatically triggers the training DAG.
   - **End-to-End Integration:** Links data preparation, training, evaluation, and deployment stages.
   - **Error Handling:** Implements robust error handling with detailed logs for debugging.

7. **Notifications and Alerts**
The pipeline keeps stakeholders informed with real-time updates:
   - **Trigger Points:** Sends notifications for data preparation completion, model training completion, evaluation results, and task failures.
   - **Bias Detection Report:** Delivers bias reports for comprehensive analysis.
   - **Alert System:** Delivers detailed emails with links to logs and results for comprehensive monitoring.


![Screenshot 2024-11-15 at 9 55 44 PM](https://github.com/user-attachments/assets/88b5c088-7d15-4584-83cc-c9676c751096)


## DAG Structure
The DAG is structured as a linear sequence of tasks, each responsible for a specific part of the model development process:

1. Data Preparation
2. Model Training
3. Model Evaluation
4. Bias Detection

### Key Components
1. Data Preparation
   - prepare_training_data_task: Prepares the training data for the model.
   - upload_to_gcs_task: Uploads the prepared data to Google Cloud Storage.

   These tasks ensure that the data is properly formatted and accessible for the training process.

2. Model Training
   - sft_train_task: Uses the SupervisedFineTuningTrainOperator to fine-tune the `gemini-1.5-flash-002` model on the prepared dataset.

   This task leverages Vertex AI for scalable training and experiment tracking. It uses supervised fine-tuning (SFT) to adapt the base model to the specific task of course registration assistance.

3. Model Evaluation
   - model_evaluation_task: Runs a comprehensive evaluation of the trained model.

   This task likely implements both standard metrics (e.g., BLEU, ROUGE) and custom metrics relevant to the course registration domain.

4. Bias Detection
The bias detection process is broken down into several subtasks:
   - get_unique_profs_task: Retrieves a list of unique professors from the dataset.
   - get_bucketed_queries_task: Generates a set of queries for evaluation.
   - get_bq_data_for_profs_task: Retrieves relevant data from BigQuery for the generated queries.
   - generate_responses_task: Uses the fine-tuned model to generate responses for the queries.
   - get_sentiment_score_task: Analyzes the sentiment of the generated responses.
   - generate_bias_report_task: Produces a comprehensive bias report based on the sentiment analysis, and sends the report in an email.


## Model Pipeline Directory Structure and Description

```
model_training/
├── README.md: Comprehensive documentation of the model pipeline, including key components and workflow.
├── __init__.py: Initializes the `model_training` package, allowing it to be imported as a module.
│
├── dags/
│   ├── __init__.py: Initializes the `dags` sub-package for Airflow workflows.
│   ├── train_eval_model_trigger_dag.py: Main Airflow DAG that orchestrates the entire model training and evaluation process.
│
├── model_scripts/
│   ├── __init__.py: Initializes the `model_scripts` sub-package.
│   │
│   ├── bias/
│   │   ├── __init__.py: Initializes the `bias` sub-package.
│   │   └── create_bias_detection_data.py: Script for generating data used in bias detection during model evaluation.
│   │
│   ├── constants/
│   │   ├── __init__.py: Initializes the `constants` sub-package.
│   │   ├── prompts.py: Contains predefined prompts used for various tasks in the pipeline.
│   │   ├── queries.py: Stores query templates used for generating evaluation data.
│   │   └── sql_queries.py: Contains SQL queries used for data retrieval from BigQuery.
│   │
│   ├── eval/
│   │   ├── __init__.py: Initializes the `eval` sub-package.
│   │   ├── custom_eval.py: Implements custom evaluation metrics for model assessment.
│   │   └── model_evaluation.py: Main script containing the logic for comprehensive model evaluation.
│   │
│   ├── train/
│   │   ├── __init__.py: Initializes the `train` sub-package.
│   │   └── prepare_dataset.py: Script for preparing and formatting datasets for model training.
│   │
│   ├── utils/
│   │   ├── __init__.py: Initializes the `utils` sub-package.
│   │   ├── data_utils.py: Utility functions for data manipulation and processing.
│   │   └── email_triggers.py: Functions for sending email notifications and alerts.
│   │
│   └── config.py: Configuration file containing global settings and parameters for the model pipeline.
│
└── tests/
    └── __init__.py: Initializes the `tests` package for unit and integration tests.
```

## Key Features
   - Automated end-to-end pipeline from data preparation to model deployment
   - Robust evaluation system with custom metrics for relevance and coverage
   - Comprehensive bias detection and reporting mechanism
   - Integration with Google Cloud Platform services for scalability and efficiency
   - Experiment tracking and version control for reproducibility and easy rollback
   
   This pipeline emphasizes automation, scalability, and fairness in model development, aligning with best practices in MLOps and responsible AI development.

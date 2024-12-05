# Model Pipeline - Key Components and Workflow

## Overview

The pipeline automates the process of fine-tuning, evaluating, and deploying a large language model (LLM) for generating course-related responses.

## Key Components

1. **Data Preparation**
The data preparation process leverages Apache Airflow, Google BigQuery, and Google Cloud Storage (GCS) to create a robust and automated workflow:
   - **End-to-End Automation**: The pipeline detects changes in the code and triggers the data preparation process without manual intervention.
   - **Data Retrieval**: An Airflow DAG queries BigQuery using predefined SQL to fetch fields like query, context, and response from relevant tables.
   - **Data Cleaning and Transformation**: The pipeline removes null or invalid entries, handles missing data, and standardizes text fields for model readiness.
   - **Data Formatting**: Converts data into JSONL format for compatibility with fine-tuning workflows, ensuring fields are aligned for supervised fine-tuning and evaluation.

2. **Model Training**
The model training process utilizes the gemini-1.5-flash-002 base model and implements supervised fine-tuning (SFT) on task-specific data:
   - **Fine-Tuning Process:** Integrates with Vertex AI for scalable training and experiment tracking.
   - **Version Control:** Implements version control for fine-tuned models in Vertex AI, allowing for easy rollback and performance tracking across different versions.

![image](https://github.com/user-attachments/assets/29f5f219-854d-4106-8170-e754d3ff611c)


3. **Model Evaluation**
The evaluation system employs both standard and custom metrics to ensure comprehensive model assessment:
   - **Standard Metrics:** BLEU, ROUGE (1, 2, L).
   - **Custom Metrics:** Relevance (degree to which the response addresses the query), Coverage (completeness and depth of the response)
   - **Bias Detection:** Implements a 5-point rubric to evaluate gender neutrality and inclusivity in responses

<img width="1408" alt="image" src="https://github.com/user-attachments/assets/e04d4507-4467-4c5d-b684-7271a2958cc7">

![image](https://github.com/user-attachments/assets/333d82fe-0fba-4c8a-918c-29120fc517e2)


4. **Model Registry**
The pipeline utilizes Vertex AI for model versioning, metadata storage, and deployment:
   - **Version Control:** Every trained model is saved as a versioned endpoint in Vertex AI, including metadata such as hyperparameters and dataset details.
   - **Experiment Tracking:** Automatically logs training parameters, metrics, and artifacts for future reference and reproducibility.
   - **Deployment Integration:** Models are directly deployable from the registry to Vertex AI endpoints.

![image](https://github.com/user-attachments/assets/8b82c967-736e-4d83-ba4a-24b5f9891453)


5. **Bias Detection and Mitigation**
The bias detection system ensures fairness and inclusivity in model responses:
   - **Query Generation:** Creates diverse prompts to test responses for bias, including questions about teaching style and approachability.
   - **Response Evaluation:** Uses sentiment analysis and manual scoring to rate responses on a 5-point rubric.
   - **Bias Report:** Aggregates results into a detailed report, highlighting patterns and areas for improvement.

![image](https://github.com/user-attachments/assets/dba9469a-31f7-43d0-8516-08959b5d4f01)

6. **CICD Pipeline**
The Continuous Integration and Continuous Deployment (CI/CD) pipeline automates the entire process from data preparation to model deployment:
   - **Triggering:** Detects updates in the code using GitHub actions and automatically triggers the training DAG.
   - **End-to-End Integration:** Links data preparation, training, evaluation, and deployment stages.
   - **Error Handling:** Implements robust error handling with detailed logs for debugging.

7. **Notifications and Alerts**
The pipeline keeps stakeholders informed with real-time updates:
   - **Trigger Points:** Sends notifications for data preparation completion, model training completion, evaluation results, and task failures.
   - **Bias Detection Report:** Delivers bias reports for comprehensive analysis.
   - **Alert System:** Delivers detailed emails with links to logs and results for comprehensive monitoring.

## DAG Structure
The DAG is structured as a linear sequence of tasks, each responsible for a specific part of the model development process:

![Screenshot 2024-11-15 at 9 55 44 PM](https://github.com/user-attachments/assets/88b5c088-7d15-4584-83cc-c9676c751096)

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


5. Model Deployment
The model deployment process ensures efficient and scalable integration with production systems using Vertex AI:  
   - **Versioned Endpoints**: Each trained model is deployed as a versioned endpoint, enabling seamless updates and rollback functionality.  
   - **Scalable Architecture**: Deployed endpoints leverage Vertex AI’s scalable infrastructure to handle varying traffic loads.  
   - **Low-Latency API Integration**: The endpoints provide low-latency APIs suitable for real-time applications.  
   - **Resource Allocation Optimization**: Automatically adjusts serving resources based on traffic patterns to minimize costs while maintaining performance.  

## Model Pipeline Directory Structure and Description

```
model_training/
├── README.md: Comprehensive documentation of the model pipeline, including key components and workflow.
├── __init__.py: Initializes the model_training package for seamless imports.
├── dags/
│   ├── __init__.py: Initializes the DAGs sub-package for Airflow workflows related to model training.
│   ├── model_scripts/
│   │   ├── __init__.py: Initializes the model_scripts sub-package for various model-related scripts.
│   │   ├── bias/
│   │   │   ├── __init__.py: Initializes the bias sub-package for handling bias detection.
│   │   │   └── create_bias_detection_data.py: Generates data used for bias detection during model evaluation.
│   │   ├── config.py: Contains configuration settings for the model pipeline.
│   │   ├── constants/
│   │   │   ├── __init__.py: Initializes the constants sub-package for model-related constants.
│   │   │   ├── prompts.py: Stores predefined prompts used in model tasks.
│   │   │   ├── queries.py: Stores query templates used for model evaluation and training.
│   │   │   └── sql_queries.py: Contains SQL queries used for retrieving model-related data from BigQuery.
│   │   ├── eval/
│   │   │   ├── __init__.py: Initializes the eval sub-package for evaluation-related tasks.
│   │   │   ├── custom_eval.py: Implements custom evaluation metrics for model assessment.
│   │   │   └── model_evaluation.py: Contains logic for comprehensive model evaluation.
│   │   ├── model_deployment/
│   │   │   └── endpoint_cleanup.py: Cleans up deployment endpoints after model deployment or retraining.
│   │   ├── model_selection/
│   │   │   └── best_model.py: Logic for selecting the best model based on evaluation results.
│   │   ├── train/
│   │   │   ├── __init__.py: Initializes the train sub-package for training-related tasks.
│   │   │   └── prepare_dataset.py: Prepares and formats datasets for model training.
│   │   └── utils/
│   │       ├── __init__.py: Initializes the utils sub-package for utility functions.
│   │       ├── data_utils.py: Utility functions for data manipulation and processing used in model tasks.
│   │       └── email_triggers.py: Functions for sending email notifications and alerts.
│   ├── tests/
│   │   └── __init__.py: Initializes the tests sub-package for unit and integration testing.
│   └── train_eval_model_trigger_dag.py: Main Airflow DAG that orchestrates the entire model training and evaluation process.
├── image-1.png: Visualization of the model pipeline.
├── image-2.png: Another diagram illustrating model selection and training steps.
├── image.png: General overview image of the model training process.
├── notebooks/
│   └── qwen_finetuning.ipynb: Jupyter notebook used for fine-tuning the Qwen model.
└── tests/
    └── __init__.py: Initializes the tests package for unit and integration tests.
└── requirements.txt: Lists Python dependencies required for the model training pipeline.


```

## Key Features
   - Automated end-to-end pipeline from data preparation to model deployment
   - Robust evaluation system with custom metrics for relevance and coverage
   - Comprehensive bias detection and reporting mechanism
   - Integration with Google Cloud Platform services for scalability and efficiency
   - Experiment tracking and version control for reproducibility and easy rollback
   
   This pipeline emphasizes automation, scalability, and fairness in model development, aligning with best practices in MLOps and responsible AI development.

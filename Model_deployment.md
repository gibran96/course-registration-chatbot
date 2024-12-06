## Comprehensive Report: Data Drift Detection and Model Training & Deployment Pipelines

### Table of Contents
1. [Executive Summary](#executive-summary)
2. [Introduction](#introduction)
3. [Data Drift Detection Pipeline](#data-drift-detection-pipeline)
    - [Architecture](#architecture)
    - [Implementation](#implementation)
    - [Workflow](#workflow)
    - [Technologies Used](#technologies-used)
    - [Challenges and Solutions](#challenges-and-solutions)
4. [Model Training and Deployment Pipeline](#model-training-and-deployment-pipeline)
    - [Architecture](#architecture-1)
    - [Implementation](#implementation-1)
    - [Workflow](#workflow-1)
    - [Technologies Used](#technologies-used-1)
    - [Challenges and Solutions](#challenges-and-solutions-1)
5. [Integration Between Pipelines](#integration-between-pipelines)
6. [Testing and Validation](#testing-and-validation)
7. [Security and Compliance](#security-and-compliance)
8. [Future Enhancements](#future-enhancements)
9. [Conclusion](#conclusion)
10. [Appendices](#appendices)

---

### Executive Summary

This report provides a comprehensive overview of the **Data Drift Detection** and **Model Training & Deployment** pipelines implemented for the Course Compass chatbot. The Data Drift Detection Pipeline ensures that the underlying data remains consistent over time, thereby maintaining model accuracy. Concurrently, the Model Training & Deployment Pipeline automates the fine-tuning, evaluation, and deployment of machine learning models using Vertex AI. Together, these pipelines facilitate a robust, scalable, and maintainable machine learning infrastructure that adapts to evolving data patterns and maintains high performance.

### Introduction

Machine learning models are highly sensitive to the quality and consistency of the data they are trained on. Over time, changes in data distributions—known as data drift—can lead to degraded model performance. To mitigate this, it is crucial to continuously monitor data for drift and update models accordingly. Additionally, automating the model training and deployment processes ensures that models remain up-to-date and performant without manual intervention.

This report details the implementation of two interconnected pipelines:

1. **Data Drift Detection Pipeline**: Monitors data for drift and triggers model retraining workflows when necessary.
2. **Model Training and Deployment Pipeline**: Automates the fine-tuning, evaluation, and deployment of machine learning models.

### Data Drift Detection Pipeline

#### Architecture

![Data Drift Detection Pipeline](https://github.com/user-attachments/assets/4a23de1b-ee54-4eb3-a824-316fee49f5ab)

The pipeline leverages Airflow for orchestration, BigQuery for data storage, Vertex AI for embedding generation, and Google Cloud Storage for data handling. The core components include data extraction, embedding generation, drift detection logic, and action triggers.

#### Implementation

**Key Components:**

- **Data Extraction**: Utilizes BigQuery to fetch training and test questions.
- **Embedding Generation**: Uses Vertex AI's `text-embedding-005` model to generate embeddings for both datasets.
- **Threshold Determination**: Calculates upper and lower similarity thresholds based on training embeddings.
- **Drift Detection**: Compares test embeddings against training embeddings to identify drift.
- **Action Triggering**: If drift is detected, triggers the `train_model_trigger_dag` for model retraining.
- **Data Archival**: Moves processed user data to archival tables in BigQuery.
- **Notification**: Sends success emails upon DAG completion.

**Scripts and Modules:**

- `bigquery_utils_data_drift.py`: Handles BigQuery interactions.
- `drift_detection.py`: Contains logic for generating embeddings, determining thresholds, and detecting drift.
- `llm_utils_data_drift.py`: Utilizes LLMs for generating responses based on drifted data.
- `gcs_utils_data_drift.py`: Manages uploads to and downloads from Google Cloud Storage.
- `backoff.py`: Implements an exponential backoff strategy for retrying failed operations.

#### Workflow

1. **Data Retrieval**: Fetches training and new user queries from BigQuery.
2. **Embedding Generation**: Generates embeddings for both datasets.
3. **Threshold Calculation**: Determines similarity thresholds based on training data.
4. **Drift Detection**: Identifies if data drift has occurred by comparing embeddings.
5. **Drift Trend Analysis**: Analyzes historical drift data to identify trends.
6. **Action Execution**: If drift is detected and a trend is confirmed, triggers model retraining.
7. **Data Archival**: Archives processed user data to maintain data hygiene.
8. **Notification**: Sends out success emails detailing the DAG execution.

#### Technologies Used

- **Apache Airflow**: Orchestrates the workflow.
- **Google BigQuery**: Stores and queries data.
- **Vertex AI**: Generates embeddings and utilizes LLMs.
- **Google Cloud Storage (GCS)**: Handles data uploads and downloads.
- **Python**: Implements the pipeline logic.
- **Logging and Monitoring Tools**: Ensures visibility into pipeline operations.

#### Challenges and Solutions

- **Handling Large Datasets**: Implemented batching mechanisms during embedding generation to manage memory and processing constraints.
- **Retry Logic**: Incorporated exponential backoff strategies to handle transient failures in API calls and data operations.
- **Threshold Sensitivity**: Fine-tuned similarity thresholds to balance sensitivity and specificity in drift detection.

### Model Training and Deployment Pipeline

#### Architecture

![Model Training and Deployment Pipeline](https://github.com/user-attachments/assets/22a92394-9b31-497e-aa42-6de7b0c76233)

This pipeline leverages Airflow for orchestration, Vertex AI for model training and evaluation, and Google Cloud Storage for data management. It integrates bias detection to ensure model fairness and reliability.

#### Implementation

**Key Components:**

- **Data Preparation**: Extracts and formats training and test data from BigQuery.
- **Supervised Fine-Tuning (SFT)**: Fine-tunes models using Vertex AI's SFT capabilities.
- **Model Evaluation**: Assesses model performance using custom metrics like BLEU and ROUGE.
- **Bias Detection**: Evaluates model responses for potential biases using sentiment analysis.
- **Model Deployment**: Deploys the best-performing model to a Vertex AI endpoint.
- **Endpoint Cleanup**: Manages endpoint lifecycle by deleting outdated models.
- **Model Comparison**: Compares new models against the best existing model to decide on deployment.

**Scripts and Modules:**

- `prepare_dataset.py`: Extracts and formats data for training.
- `model_evaluation.py`: Runs evaluations using custom metrics.
- `create_bias_detection_data.py`: Generates data for bias evaluation.
- `best_model.py`: Compares current and best models to determine deployment.
- `endpoint_cleanup.py`: Handles deployment and cleanup of model endpoints.
- `email_triggers.py`: Manages email notifications for bias reports.

#### Workflow

1. **Data Preparation**: Extracts training and test data from BigQuery, cleans, and formats it into JSONL.
2. **Upload to GCS**: Uploads prepared data to Google Cloud Storage.
3. **Supervised Fine-Tuning**: Fine-tunes the model using the training dataset.
4. **Model Evaluation**: Evaluates the fine-tuned model against test data using BLEU and ROUGE metrics.
5. **Bias Detection**:
   - Retrieves unique professors and generates evaluation queries.
   - Generates model responses and analyzes sentiment to detect bias.
   - Compiles bias reports and sends notifications if bias is detected.
6. **Endpoint Cleanup**: Deletes the default Vertex AI endpoint post-deployment.
7. **Model Comparison**: Compares the new model's metrics against the best existing model.
8. **Model Deployment**: Deploys the new model if it outperforms the existing one.
9. **Notification**: Sends success emails upon DAG completion.

#### Technologies Used

- **Apache Airflow**: Orchestrates the workflow.
- **Google BigQuery**: Stores and queries data.
- **Vertex AI**: Handles model fine-tuning, evaluation, and deployment.
- **Google Cloud Storage (GCS)**: Manages data uploads and downloads.
- **Python**: Implements the pipeline logic.
- **TextBlob & Vertex AI Generative Models**: Conduct sentiment analysis and response generation.
- **Logging and Monitoring Tools**: Ensures visibility into pipeline operations.

#### Challenges and Solutions

- **Bias Detection Complexity**: Utilized advanced sentiment analysis and LLMs to accurately detect and report biases.
- **Model Comparison Metrics**: Defined clear metrics (BLEU, ROUGE) to objectively compare model performances.
- **Endpoint Management**: Automated endpoint cleanup to prevent resource bloat and ensure optimal deployment practices.

### Integration Between Pipelines

The **Data Drift Detection** and **Model Training & Deployment** pipelines are seamlessly integrated to ensure that any detected drift triggers appropriate model updates. When data drift is identified, the Data Drift Detection Pipeline triggers the `train_model_trigger_dag`, initiating the Model Training & Deployment Pipeline. This integration ensures continuous model improvement and adaptation to evolving data patterns.

### Testing and Validation

- **Unit Tests**: Implemented for individual scripts and functions to ensure correctness.
- **Integration Tests**: Verified the end-to-end workflow across both pipelines.
- **Performance Tests**: Assessed the pipelines' ability to handle large datasets and multiple concurrent runs.
- **Bias Detection Validation**: Conducted manual reviews to validate the accuracy of bias detection mechanisms.

### Security and Compliance

- **Authentication**: Ensured secure access to Google Cloud resources using service accounts with least privilege.
- **Data Privacy**: Complied with data protection regulations by anonymizing sensitive information during processing.
- **Secure Storage**: Stored data securely in BigQuery and GCS with appropriate access controls.

### Future Enhancements

- **Automated Threshold Tuning**: Implement dynamic threshold adjustments based on historical drift data.
- **Advanced Bias Metrics**: Incorporate more nuanced bias detection metrics and expand to other forms of bias beyond sentiment.
- **Real-time Drift Detection**: Transition from batch processing to real-time monitoring for immediate drift detection.
- **Scalability Improvements**: Optimize pipeline components to handle larger datasets and higher throughput.

### Conclusion

The implemented **Data Drift Detection** and **Model Training & Deployment** pipelines establish a robust framework for maintaining and improving machine learning models. By automating drift detection, bias evaluation, and model updates, these pipelines ensure that the Course Compass chatbot remains accurate, fair, and reliable, providing a high-quality user experience.

---

## Additional Information

### Repository Links

- **Data Drift Pipeline Repository**: [GitHub - Data Drift](https://www.github.com/gibran96/course-registration-chatbot/data_drift/)
- **Model Training and Deployment Pipeline Repository**: [GitHub - Model Training](https://www.github.com/gibran96/course-registration-chatbot/model_training/)

### Contribution Guidelines

1. **Fork the Repository**: Create a personal copy of the repository.
2. **Create a Feature Branch**: Develop your feature or fix in a separate branch.
3. **Commit Changes**: Ensure your commits are clear and descriptive.
4. **Open a Pull Request**: Submit your changes for review.
5. **Review Process**: Collaborate with maintainers to refine and merge your contributions.

### License

This project is licensed under the [MIT License](LICENSE).

### Contact

For any questions or support, please reach out to [mlopsggmu@gmail.com](mailto:mlopsggmu@gmail.com).
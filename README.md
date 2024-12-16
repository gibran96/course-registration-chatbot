
# **Course Compass MLOps System**

## **Overview**
The Course Compass project is a comprehensive MLOps system designed to enhance the course registration process for college students. It integrates data collection, processing, and machine learning operations to provide real-time, accurate, and insightful responses about courses, instructors, and academic requirements.

The system uses cutting-edge tools and technologies, including Google Cloud Platform (GCP), Apache Airflow, Vertex AI, and BigQuery, to ensure scalability, automation, and robustness across all workflows.

---

## **Problem Statement**
Traditional course registration systems often lack automation and personalized insights, leading to challenges such as:
- Limited context-aware responses about course requirements and instructor reviews.
- Outdated or inconsistent data impacting user decisions.
- Reduced accuracy of machine learning models due to data drift over time.
- High manual effort for model updates and data processing.

---

## **Solution**
The Course Compass project addresses these challenges through:
1. **Automated Data Pipelines**:
   - Collects and processes data from various sources, such as Banner (course information) and TRACE (instructor feedback).
   - Cleans and structures the data for model training and user insights.

2. **Data Drift Detection**:
   - Monitors incoming data for drift compared to the training data distribution.
   - Automatically triggers model retraining workflows to maintain performance.

3. **Machine Learning Pipelines**:
   - Fine-tunes large language models (LLMs) for personalized, context-aware responses.
   - Incorporates advanced evaluation metrics and bias detection for fairness and accuracy.

4. **Model Deployment and Monitoring**:
   - Deploys fine-tuned models to scalable endpoints on Vertex AI.
   - Continuously monitors model performance and automates updates.

5. **Integration with Modern Tools**:
   - Leverages Apache Airflow for orchestration, Vertex AI for model operations, and BigQuery for efficient data storage and querying.

---

## **System Architecture**
The system is divided into three major pipelines:

### 1. **Data Pipeline**

![diagram-export-07-12-2024-15_21_39](https://github.com/user-attachments/assets/866be8a0-4ec5-4b30-b02c-663b9093f567)

- **Sources**: Fetches course information (Banner), student feedback (TRACE PDFs), and historical training data (BigQuery).
- **Processing**: Extracts, cleans, and transforms data into structured datasets for downstream use.
- **Outputs**: 
  - Enriched training datasets in JSONL format.
  - Archived data in GCS and BigQuery for analysis.

### 2. **Model Training and Deployment Pipeline**

![diagram-export-07-12-2024-15_25_16](https://github.com/user-attachments/assets/f0a1f891-a486-485c-bc20-a7eb4cef5661)

- **Training**:
  - Fine-tunes base models with task-specific data.
  - Evaluates models using metrics like BLEU, ROUGE, and custom relevance metrics.
- **Deployment**:
  - Deploys the best-performing model to a Vertex AI endpoint.
  - Ensures scalability and low-latency response times.
 
### 3. **Data Drift Detection Pipeline**

![diagram-export-07-12-2024-15_26_04](https://github.com/user-attachments/assets/ad54d7c5-f44e-4ade-930b-a0d5ee890b54)

- Monitors incoming queries for distributional changes compared to training data.
- Triggers retraining workflows upon detecting significant drift.
- Archives processed data and logs drift trends in BigQuery.

---

## **Features**
- **Automated End-to-End Workflows**: From data collection to model deployment.
- **Drift Detection and Retraining**: Ensures models stay relevant and accurate.
- **Bias Detection**: Evaluates model responses for fairness and inclusivity.
- **CI/CD Integration**: Automates updates and deployments using GitHub Actions.
- **Comprehensive Logging and Monitoring**: Tracks pipeline performance and errors.

---

## **Technologies Used**
- **Google Cloud Platform (GCP)**: Infrastructure for storage, computation, and ML workflows.
- **Vertex AI**: Fine-tuning, evaluation, and deployment of machine learning models.
- **BigQuery**: Scalable storage and querying for structured datasets.
- **Apache Airflow**: Workflow orchestration and pipeline automation.
- **Python**: Core programming language for all pipeline logic.

---

## **Project Directory Structure**
```
├── data_pipeline/
│   ├── dags/                # Airflow DAGs for data extraction and processing
│   ├── scripts/             # Data processing scripts
│   ├── requirements.txt     # Python dependencies for data pipeline
│
├── drift_detection/
│   ├── dags/                # Airflow DAGs for drift detection
│   ├── scripts/             # Drift detection and threshold calculation scripts
│
├── model_training/
│   ├── dags/                # Airflow DAGs for training and deployment
│   ├── model_scripts/       # Training, evaluation, and deployment scripts
│   ├── requirements.txt     # Python dependencies for model pipeline
│
├── backend/
│   ├── app/                 # FastAPI app for serving responses
│   ├── requirements.txt     # Python dependencies for backend
│
├── README.md                # Project documentation
```


## **Future Enhancements**
- **Real-Time Drift Detection**: Transition from batch to real-time monitoring.
- **Advanced Bias Metrics**: Include diverse evaluation metrics for model fairness.

---

## **Contributors**
- [Goutham Yadavalli](mailto:yadavalli.s@northeastern.edu)
- [Gibran Myageri](mailto:myageri.g@northeastern.edu)
- [Mihir Athale](mailto:athale.m@northeastern.edu)
- [Rushikesh Ghatage](mailto:ghatage.r@northeastern.edu)
- [Kishore Sampath](mailto:kishore.sampath@neu.edu)

For questions or issues, please contact the contributors or raise an issue in the repository.

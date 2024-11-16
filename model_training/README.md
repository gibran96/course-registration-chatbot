# Model Pipeline - Key Components and Workflow

### 1. Loading Data from the Data Pipeline for Model Training

**Overview**: Automates data extraction, cleaning, and preparation for training using **Apache Airflow**, **Google BigQuery**, and **Google Cloud Storage (GCS)**.

**Key Features**:
- **End-to-End Automation**: Detects new data in BigQuery and triggers data preparation without manual intervention.
- **Real-Time Updates**: Ensures training is based on the latest data for relevance and accuracy.
- **Cloud Integration**: Scalable querying with BigQuery and efficient storage with GCS.

**Workflow**:
1. **Data Retrieval**:
   - Airflow DAG queries BigQuery using pre-defined SQL.
   - Fetches fields like `query`, `context`, and `response` from relevant tables.
   - Ensures data integrity by validating schema compliance.
2. **Data Cleaning and Transformation**:
   - Removes null or invalid entries.
   - Handles missing data using imputation strategies if required.
   - Standardizes text fields for model readiness.
3. **Data Formatting**:
   - Converts data into **JSONL** for compatibility with fine-tuning workflows.
   - Ensures fields are aligned for supervised fine-tuning and evaluation (e.g., `instruction`, `context`, `expected response`).
4. **Storage and Accessibility**:
   - Processed data is versioned and stored locally.
   - Automatically uploads datasets to GCS with metadata (e.g., timestamp, batch ID).

**Triggering**:
- The DAG is triggered dynamically on new data detection in BigQuery, ensuring the pipeline is always aligned with the latest information.

### 2. Training and Selecting the Best Model

**Overview**: Automates fine-tuning of pre-trained models, integrating with Vertex AI and Airflow.

**Key Components**:
- **Fine-Tuning**:
  - Base model: `gemini-1.5-flash-002`.
  - Supervised fine-tuning (SFT) on task-specific data from GCS.
- **Metrics**:
  - Standard: BLEU, ROUGE (1, 2, L), groundedness, instruction following.
  - Custom: Bias detection and fairness metrics.
- **Version Control**:
  - Fine-tuned models are versioned in Vertex AI for easy rollback or performance tracking.

**Workflow**:
1. **Training Data Preparation**:
   - Formats and cleans data in JSONL.
   - Uploads datasets to GCS with metadata for traceability.
2. **Fine-Tuning**:
   - Initiates training on Vertex AI, leveraging pre-configured pipelines.
   - Records training hyperparameters, data versions, and logs for reproducibility.
3. **Evaluation and Selection**:
   - Evaluates performance using test datasets.
   - Models are ranked using a weighted scoring system that considers both standard and custom metrics.
   - Automatically selects the best-performing model for deployment.
4. **Bias Detection**:
   - Incorporates additional tests to measure bias, fairness, and inclusivity.
   - Generates a bias report detailing areas of improvement.
5. **Model Registry Update**:
   - Saves the selected model to Vertex AI's registry with associated metadata, evaluation results, and experiment details.

**Key Features**:
- Automates the entire training process from data preparation to model deployment.
- Ensures fairness with built-in bias detection.
- Supports model versioning for traceability and rollback.

![Screenshot 2024-11-15 at 9 55 44 PM](https://github.com/user-attachments/assets/88b5c088-7d15-4584-83cc-c9676c751096)


### 3. Model Evaluation

**Overview**: Evaluates fine-tuned models using rigorous metrics to ensure quality and fairness.

**Key Components**:
- **Dataset**:
  - Test data prepared from the data pipeline.
  - Structured as `instruction`, `context`, and `expected response`.
- **Metrics**:
  - **Standard**:
    - BLEU: Measures textual similarity.
    - ROUGE: Measures n-gram overlap.
    - Exact Match: Percentage of fully correct responses.
  - **Custom**:
    - Relevance: Degree to which the response addresses the query.
    - Coverage: Completeness and depth of the response.
- **Bias Detection**:
  - Evaluates gender neutrality and inclusivity in responses.
  - Uses a 5-point rubric to score bias levels.

**Workflow**:
1. **Trigger Post-Training**:
   - DAG waits for training completion before starting evaluation.
   - Retrieves the trained model version from Vertex AI.
2. **Generate Predictions**:
   - Runs the model on the test dataset to generate predictions.
   - Uses structured prompts to ensure consistency in evaluation.
3. **Calculate Metrics**:
   - Computes standard and custom metrics for each response.
   - Aggregates results into a summary report.
4. **Bias Detection**:
   - Analyzes responses for neutrality and inclusivity.
   - Generates a detailed bias report with recommendations.
5. **Result Storage**:
   - Saves evaluation results locally and uploads to GCS for integration with dashboards.

**Key Features**:
- Dynamic selection of the latest trained model.
- Comprehensive evaluation combining standard and custom metrics.
- Scalable to handle large datasets and complex models.

### 4. Model Bias Detection

**Overview**: Ensures the model's responses are free from gender bias and uphold inclusivity.

**Bias Detection Criteria**:
- **Gender Bias Presence**: Detects tendencies toward specific gender preferences or stereotypes.
- **Neutrality**: Measures if responses avoid gendered language.
- **Inclusivity**: Ensures diverse and respectful representation.

**Implementation**:
1. **Query Generation**:
   - Creates diverse prompts to test responses for bias.
   - Examples include: "What is the teaching style of {prof_name}?" or "How approachable is {prof_name}?"
2. **Response Evaluation**:
   - Uses sentiment analysis and manual scoring.
   - Rates responses on a 5-point rubric (1 = strongly biased, 5 = completely neutral).
3. **Bias Report**:
   - Aggregates results into a detailed report.
   - Highlights patterns and areas for improvement.


### 5. Model Registry & Experiment Tracking

#### **Overview**
The Model Registry & Experiment Tracking ensures version control, traceability, and streamlined deployment of models. Using **Vertex AI**, the registry logs all artifacts and metadata.

#### **Key Features**

##### **Version Control**
- Every trained model is saved as a versioned endpoint in Vertex AI.
- Includes:
  - **Metadata:** Hyperparameters, dataset details, etc.
  - **Performance metrics.**

##### **Experiment Tracking**
- Automatically logs:
  - **Training parameters:** Learning rate, optimizer, epochs, etc.
  - **Metrics:** BLEU, ROUGE, bias scores.
  - **Artifacts:** Training and test datasets, evaluation reports.

##### **Reproducibility**
- Stores artifacts for future retraining or debugging.
- Supports hyperparameter optimization and comparison.

##### **Deployment Integration**
- Models are directly deployable from the registry to Vertex AI endpoints.

### 6. CI/CD for Model Training

**Overview**: Implements automated Continuous Integration and Continuous Deployment.

**Workflow**:
1. **Triggering**:
   - Detects data updates in BigQuery and triggers the training DAG.
2. **End-to-End Integration**:
   - Links data preparation, training, evaluation, and deployment.
3. **Deployment**:
   - Deploys the best-performing model to Vertex AI endpoints.
4. **Error Handling**:
   - Alerts on task failures with detailed logs for debugging.


### 7. Notifications & Alerts

**Overview**: Keeps stakeholders informed with real-time updates.

**Integration**:
- **Trigger Points**:
  - Data preparation completion.
  - Model training completion.
  - Evaluation results.
  - Task failures.
- **Alerts**:
  - Sends detailed emails with links to logs and results.


## Model Pipeline Directory Structure and Description

```
│
├── model_training
│   ├── __init__.py: Initializes the `model_training` package.
│
│   ├── dags
│       ├── __init__.py: Initializes the `dags` sub-package for Airflow workflows related to model training.
│       ├── model_evaluation_dag.py: Airflow DAG for evaluating trained models.
│       ├── train_model_trigger_tag.py: Airflow DAG for triggering model training workflows.
│       ├── model_scripts
│           ├── __init__.py: Initializes the `model_scripts` sub-package.
│           ├── config.py: Configuration file for model training parameters.
│           ├── create_bias_detection_data.py: Generates data for bias detection during training.
│           ├── custom_eval.py: Contains custom evaluation metrics for models.
│           ├── data_utils.py: Utility functions for preparing and handling training datasets.
│           ├── model_eval.py: Functions for running and reporting model evaluations.
│           ├── model_evaluation.py: Main script for model evaluation logic.
│           ├── prepare_dataset.py: Prepares datasets for training and evaluation.
│           └── prompts.py: Stores prompts for generating synthetic data using LLMs.
```

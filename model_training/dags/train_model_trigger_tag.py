from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from airflow.operators.email import EmailOperator
import os
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import (
    SupervisedFineTuningTrainOperator,
    RunEvaluationOperator,
)
from vertexai.generative_models import HarmBlockThreshold, HarmCategory, Part, Tool, grounding
from vertexai.preview.evaluation import MetricPromptTemplateExamples
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator
)
from model_scripts.prepare_dataset import prepare_training_data
from model_scripts.data_utils import upload_to_gcs
from airflow.models import Variable
import datetime
from model_scripts.prompts import PROMPT_TEMPLATE
from uuid import uuid4
from model_scripts.custom_metrics import (
    AnswerRelevanceMetric, 
    AnswerCoverageMetric, 
    CustomMetricConfig,
    aggregate_metrics
)


PROJECT_ID = os.environ.get("PROJECT_ID", "coursecompass")

REGION = Variable.get("REGION", "us-east1")

SOURCE_MODEL = Variable.get("SOURCE_MODEL","gemini-1.5-flash-002")

TRAIN_DATASET = Variable.get("TRAIN_DATASET","gs://mlops-data-7374/finetuning_data.jsonl")

TUNED_MODEL_DISPLAY_NAME = Variable.get("TUNED_MODEL_DISPLAY_NAME","course_registration_gemini_1_5_flash_002")

METRICS = [
    MetricPromptTemplateExamples.Pointwise.SUMMARIZATION_QUALITY,
    MetricPromptTemplateExamples.Pointwise.GROUNDEDNESS,
    MetricPromptTemplateExamples.Pointwise.VERBOSITY,
    MetricPromptTemplateExamples.Pointwise.INSTRUCTION_FOLLOWING,
    "exact_match",
    "bleu",
    "rouge_1",
    "rouge_2",
    "rouge_l_sum",
]

EXPERIMENT_NAME = "eval-name" + str(uuid4().hex)[:3]
EXPERIMENT_RUN_NAME = "eval-run" + str(uuid4().hex)[:3]


def run_model_evaluation(**context):
    pretrained_model = context["ti"].xcom_pull(task_ids="sft_train_task")["tuned_model_endpoint_name"]
    eval_dataset = context["ti"].xcom_pull(task_ids="prepare_training_data", key="test_data")
    test_file_name = context["ti"].xcom_pull(task_ids="upload_to_gcs", key="uploaded_test_file_path")
    logging.info(f"Test file name: {test_file_name}")
    # test_file_name = "gs://mlops-data-7374/test_data.jsonl"

    logging.info(f"Pretrained model: {pretrained_model}")
    # logging.info(f"Evaluation dataset: {eval_dataset}")

    run_eval = RunEvaluationOperator(
        task_id="model_evaluation_task_inside_dag",
        project_id=PROJECT_ID,
        location=REGION,
        pretrained_model=pretrained_model,
        metrics=METRICS,
        prompt_template=PROMPT_TEMPLATE,
        eval_dataset=test_file_name,
        experiment_name=EXPERIMENT_NAME,
        experiment_run_name=EXPERIMENT_RUN_NAME,
    )

    run_eval.execute(context)

def run_custom_evaluation(**context):
    """Run custom evaluation metrics"""
    try:
        # Get model name and evaluation dataset
        model_name = context['task_instance'].xcom_pull(key='model_name')
        eval_dataset = context['task_instance'].xcom_pull(task_ids='prepare_eval_data')
        
        # Initialize evaluation model
        evaluation_model = model_name
        
        # Initialize metrics
        config = CustomMetricConfig()
        relevance_metric = AnswerRelevanceMetric(evaluation_model, config)
        coverage_metric = AnswerCoverageMetric(evaluation_model, config)
        
        # Load evaluation dataset
        with open(eval_dataset, 'r') as f:
            examples = [json.loads(line) for line in f]
        
        # Run evaluations
        relevance_results = []
        coverage_results = []
        
        for example in examples:
            relevance_results.append(relevance_metric.evaluate_example(example))
            coverage_results.append(coverage_metric.evaluate_example(example))
        
        # Aggregate results
        all_results = relevance_results + coverage_results
        aggregated_metrics = aggregate_metrics(all_results)
        
        # Save detailed results
        results = {
            "model_name": model_name,
            "timestamp": context['ts'],
            "aggregated_metrics": aggregated_metrics,
            "detailed_results": [
                {
                    "example_id": idx,
                    "relevance": rel.value,
                    "coverage": cov.value,
                    "relevance_explanation": rel.metadata["explanation"],
                    "coverage_explanation": cov.metadata["explanation"],
                    "question": rel.metadata["question"],
                    "answer": rel.metadata["answer"]
                }
                for idx, (rel, cov) in enumerate(zip(relevance_results, coverage_results))
            ]
        }
        
        # Save results
        context['task_instance'].xcom_push(key='custom_metrics', value=results)
        return results
        
    except Exception as e:
        logging.error(f"Error running custom evaluation: {e}")
        raise

# Add to your DAG

with DAG(
    "train_model_trigger_dag",
    schedule_interval=None,
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    tags=["vertex-ai"],
) as dag:
    
    prepare_training_data_task = PythonOperator(
        task_id="prepare_training_data",
        python_callable=prepare_training_data,
        provide_context=True,
    )

    upload_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
        provide_context=True,
    )

    sft_train_task = SupervisedFineTuningTrainOperator(
        task_id="sft_train_task",
        project_id=PROJECT_ID,
        location=REGION,
        source_model=SOURCE_MODEL,
        train_dataset=TRAIN_DATASET,
        tuned_model_display_name=TUNED_MODEL_DISPLAY_NAME + str(datetime.datetime.now().strftime("%Y%m%d%H%M%S")),
        epochs=1,
        adapter_size=1,
        learning_rate_multiplier=1.0,
    )

    # model_evaluation_task = RunEvaluationOperator(
    #     task_id="model_evaluation_task",
    #     project_id=PROJECT_ID,
    #     location=REGION,
    #     pretrained_model='{{ task_instance.xcom_pull(task_ids="sft_train_task")["tuned_model_name"] }}',
    #     metrics=METRICS,
    #     prompt_template=INSTRUCTION_PROMPT,
    #     eval_dataset='{{ task_instance.xcom_pull(task_ids="prepare_training_data", key="test_data") }}',
    #     experiment_name=EXPERIMENT_NAME,
    #     experiment_run_name=EXPERIMENT_RUN_NAME,
    #     provided_context=True,
    # )


    model_evaluation_task = PythonOperator(
        task_id="model_evaluation_task",
        python_callable=run_model_evaluation,
        provide_context=True,
    )
    
    custom_evaluation_task = PythonOperator(
    task_id='custom_evaluation_task',
    python_callable=run_custom_evaluation,
    provide_context=True,
    )

     # Add task to trigger evaluation DAG
    # trigger_evaluation = TriggerDagRunOperator(
    #     task_id='trigger_evaluation',
    #     trigger_dag_id='model_evaluation_dag',
    #     conf={
    #         'training_dag_id': 'train_model_trigger_dag',
    #         'training_run_id': '{{ run_id }}',
    #         'model_name': '{{ task_instance.xcom_pull(task_ids="sft_train_task")["tuned_model_name"] }}'
    #     }
    # )

    prepare_training_data_task >> upload_to_gcs_task >> sft_train_task >> model_evaluation_task >> custom_evaluation_task


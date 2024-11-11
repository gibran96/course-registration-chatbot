from __future__ import annotations

import os
from datetime import datetime

from vertexai.generative_models import HarmBlockThreshold, HarmCategory, Part, Tool, grounding
from vertexai.preview.evaluation import MetricPromptTemplateExamples

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import (
    RunEvaluationOperator,
)
from model_scripts.prompts import INSTRUCTION_PROMPT


GENERATION_CONFIG = {"max_output_tokens": 1024, "top_p": 0.95, "temperature": 0.0}

SAFETY_SETTINGS = {
    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
}

EVAL_DATASET = 
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

EXPERIMENT_NAME = "eval-experiment-airflow-operator"

EXPERIMENT_RUN_NAME = "eval-experiment-airflow-operator-run"

PROMPT_TEMPLATE = "{instruction}. Article: {context}. Summary:"


CACHED_MODEL = "gemini-1.5-pro-002"

CACHED_SYSTEM_INSTRUCTION = """
You are an expert researcher. You always stick to the facts in the sources provided, and never make up new facts.
Now look at these research papers, and answer the following questions.
"""


CACHED_CONTENTS = [
    Part.from_uri(
        "gs://cloud-samples-data/generative-ai/pdf/2312.11805v3.pdf",
        mime_type="application/pdf",
    ),
    Part.from_uri(
        "gs://cloud-samples-data/generative-ai/pdf/2403.05530.pdf",
        mime_type="application/pdf",
    ),
]


with DAG(
    dag_id=DAG_ID,
    description="Sample DAG with generative models.",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "generative_model"],
) as dag:
    

    run_evaluation_task = RunEvaluationOperator(
        task_id="run_evaluation_task",
        project_id=PROJECT_ID,
        location=REGION,
        pretrained_model=MULTIMODAL_MODEL,
        eval_dataset=EVAL_DATASET,
        metrics=METRICS,
        experiment_name=EXPERIMENT_NAME,
        experiment_run_name=EXPERIMENT_RUN_NAME,
        prompt_template=PROMPT_TEMPLATE,
    )

from __future__ import annotations
import os
from datetime import datetime
import json
import logging
from uuid import uuid4
from vertexai.language_models import TextGenerationModel
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from vertexai.generative_models import HarmBlockThreshold, HarmCategory
from model_scripts.constants.prompts import INSTRUCTION_PROMPT
from vertexai.preview.evaluation import PointwiseMetric, PointwiseMetricPromptTemplate, EvalTask
from model_scripts.constants.prompts import BIAS_PROMPT_TEMPLATE, PROMPT_TEMPLATE
import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig

GENERATION_CONFIG = {"max_output_tokens": 1024, "top_p": 0.95, "temperature": 0.0}
PROJECT_ID = os.environ.get("PROJECT_ID", "coursecompass")

SAFETY_SETTINGS = {
    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
}

custom_bias_check = PointwiseMetric(
    metric="custom-bias-check",
    metric_prompt_template=PointwiseMetricPromptTemplate(
        criteria=BIAS_PROMPT_TEMPLATE["criteria"],
        rating_rubric=BIAS_PROMPT_TEMPLATE["rating_rubric"],
        instruction=BIAS_PROMPT_TEMPLATE["instruction"],
        evaluation_steps=BIAS_PROMPT_TEMPLATE["evaluation_steps"],
        metric_definition=BIAS_PROMPT_TEMPLATE["metric_definition"]
    ),
)

METRICS = [
    "bleu",
    "rouge_l_sum",
]

EXPERIMENT_NAME = "eval-name" + str(uuid4().hex)[:3]
EXPERIMENT_RUN_NAME = "eval-run" + str(uuid4().hex)[:3]

def run_model_evaluation(**context):
    """
    Executes the model evaluation process by performing the following steps:
    
    1. Retrieves the trained model's endpoint name and test dataset from XCom.
    2. Initializes the Vertex AI environment with the specified project and location.
    3. Configures the GenerativeModel with safety settings and generation configuration.
    4. Creates an evaluation task using the test dataset and specified metrics.
    5. Evaluates the model using the EvalTask and stores the results.
    6. Pushes evaluation results to XCom for downstream tasks to access.

    Args:
        context (dict): Airflow context dictionary containing task instance and other runtime information.

    Returns:
        list: Evaluation results containing metric scores and other relevant information.
    """
    pretrained_model = context["ti"].xcom_pull(task_ids="sft_train_task")["tuned_model_endpoint_name"]
    test_file_name = context["ti"].xcom_pull(task_ids="upload_to_gcs", key="uploaded_test_file_path")
    logging.info(f"Test file name: {test_file_name}")

    logging.info(f"Pretrained model: {pretrained_model}")

    vertexai.init(project=PROJECT_ID, location="us-central1")

    model = GenerativeModel(
        model_name=pretrained_model,
        safety_settings={
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
        },
        generation_config=GenerationConfig(
            max_output_tokens=8192,
            temperature=0.7,
        ),
    )
    logging.info(f"Model created")
    eval_results = []

    eval_task = EvalTask(
        dataset=test_file_name,
        metrics=METRICS,
        experiment=EXPERIMENT_NAME,
    )

    eval_results = eval_task.evaluate(
        model=model,
        prompt_template=PROMPT_TEMPLATE,
        experiment_run_name=EXPERIMENT_RUN_NAME,
        evaluation_service_qps=0.2,
        retry_timeout=1,
    )

    context['ti'].xcom_push(key='eval_result', value=eval_results)
    context['ti'].xcom_push(key='experiment_name', value=EXPERIMENT_NAME)
    context['ti'].xcom_push(key='experiment_run_name', value=EXPERIMENT_RUN_NAME)

    return eval_results

# def load_test_data(**context):
#     """Load test data from the file created by prepare_dataset.py"""
#     test_file = context['ti'].xcom_pull(task_ids='prepare_training_data', key='test_data_file_path')
    
#     if not os.path.exists(test_file):
#         raise FileNotFoundError(f"Test file not found at {test_file}")
        
#     with open(test_file, 'r') as f:
#         test_data = [json.loads(line) for line in f]
        
#     # Convert the JSONL format to evaluation format
#     eval_data = []
#     for item in test_data:
#         # Extract the components from your existing format
#         user_query = item['contents'][0]['parts'][0]['text']
#         expected_response = item['contents'][1]['parts'][0]['text']
        
#         # Parse the instruction and context from the formatted prompt
#         instruction_start = user_query.find("Question:") + 9
#         context_start = user_query.find("Context:") + 8
        
#         instruction = user_query[instruction_start:context_start].strip()
#         context = user_query[context_start:].strip()
        
#         eval_data.append({
#             "instruction": instruction,
#             "context": context,
#             "expected_response": expected_response
#         })
    
#     return eval_data

# def calculate_metrics(predictions, references):
#     """Calculate evaluation metrics"""
#     scorer = rouge_scorer.RougeScorer(['rouge1', 'rouge2', 'rougeL'], use_stemmer=True)
#     #bleu = evaluate.load('bleu')
    
#     # Calculate ROUGE scores
#     rouge_scores = {metric: [] for metric in ['rouge1', 'rouge2', 'rougeL']}
    
#     for pred, ref in zip(predictions, references):
#         scores = scorer.score(pred, ref)
#         for metric in rouge_scores:
#             rouge_scores[metric].append(scores[metric].fmeasure)
    
#     # Calculate BLEU
#     #bleu_score = bleu.compute(predictions=predictions, references=[[r] for r in references])
    
#     # Calculate exact matches
#     exact_matches = [1 if p.strip() == r.strip() else 0 for p, r in zip(predictions, references)]
    
#     return {
#         'rouge1': np.mean(rouge_scores['rouge1']),
#         'rouge2': np.mean(rouge_scores['rouge2']),
#         'rougeL': np.mean(rouge_scores['rougeL']),
#         #'bleu': bleu_score['bleu'],
#         'exact_match': np.mean(exact_matches)
#     }

# def run_evaluation(**context):
    """Main evaluation function"""
    logging.info("Starting model evaluation")
    
    # Load test data
    eval_data = load_test_data(**context)
    logging.info(f"Loaded {len(eval_data)} test examples")
    
    # Initialize model
    model_name = context['task_instance'].xcom_pull(task_ids='get_latest_model_task')
    model = TextGenerationModel.from_pretrained(model_name)
    logging.info(f"Loaded model {model_name}")
    
    predictions = []
    references = []
    
    # Generate predictions for each test example
    for item in eval_data:
        # Format the prompt using your existing template
        prompt = INSTRUCTION_PROMPT.format(
            query=item['instruction'],
            content=item['context']
        )
        
        # Generate prediction
        try:
            prediction = model.predict(
                prompt,
                temperature=GENERATION_CONFIG['temperature'],
                max_output_tokens=GENERATION_CONFIG['max_output_tokens'],
                top_p=GENERATION_CONFIG['top_p']
            ).text
            predictions.append(prediction)
            references.append(item['expected_response'])
        except Exception as e:
            logging.error(f"Error generating prediction: {e}")
            continue
    
    # Calculate metrics
    metrics = calculate_metrics(predictions, references)
    
    # Save results
    results = {
        'metrics': metrics,
        'evaluation_samples': [
            {
                'instruction': item['instruction'],
                'context': item['context'],
                'expected': item['expected_response'],
                'predicted': pred
            }
            for item, pred in zip(eval_data[:5], predictions[:5])  # Save first 5 examples
        ]
    }
    
    # Save locally first
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    local_path = f"tmp/eval_results_{timestamp}.json"
    os.makedirs("tmp", exist_ok=True)
    
    with open(local_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    # Upload to GCS
    bucket_name = context['dag_run'].conf.get('bucket_name', Variable.get('default_bucket_name'))
    gcs_hook = GCSHook()
    gcs_path = f"eval_results/eval_results_{timestamp}.json"
    
    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=gcs_path,
        filename=local_path
    )
    
    logging.info(f"Evaluation results saved to GCS: gs://{bucket_name}/{gcs_path}")
    logging.info(f"Metrics: {metrics}")
    
    return metrics
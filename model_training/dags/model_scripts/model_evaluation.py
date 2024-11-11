from __future__ import annotations
import os
from datetime import datetime
import json
import logging
from google.cloud import aiplatform
from vertexai.language_models import TextGenerationModel
import numpy as np
from rouge_score import rouge_scorer
import evaluate
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from vertexai.generative_models import HarmBlockThreshold, HarmCategory
from model_scripts.prompts import INSTRUCTION_PROMPT, SYSTEM_INSTRUCTION


GENERATION_CONFIG = {"max_output_tokens": 1024, "top_p": 0.95, "temperature": 0.0}

SAFETY_SETTINGS = {
    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
}

def load_test_data(**context):
    """Load test data from the file created by prepare_dataset.py"""
    test_file = context['ti'].xcom_pull(task_ids='prepare_training_data', key='test_data_file_path')
    
    if not os.path.exists(test_file):
        raise FileNotFoundError(f"Test file not found at {test_file}")
        
    with open(test_file, 'r') as f:
        test_data = [json.loads(line) for line in f]
        
    # Convert the JSONL format to evaluation format
    eval_data = []
    for item in test_data:
        # Extract the components from your existing format
        user_query = item['contents'][0]['parts'][0]['text']
        expected_response = item['contents'][1]['parts'][0]['text']
        
        # Parse the instruction and context from the formatted prompt
        instruction_start = user_query.find("Question:") + 9
        context_start = user_query.find("Context:") + 8
        
        instruction = user_query[instruction_start:context_start].strip()
        context = user_query[context_start:].strip()
        
        eval_data.append({
            "instruction": instruction,
            "context": context,
            "expected_response": expected_response
        })
    
    return eval_data

def calculate_metrics(predictions, references):
    """Calculate evaluation metrics"""
    scorer = rouge_scorer.RougeScorer(['rouge1', 'rouge2', 'rougeL'], use_stemmer=True)
    bleu = evaluate.load('bleu')
    
    # Calculate ROUGE scores
    rouge_scores = {metric: [] for metric in ['rouge1', 'rouge2', 'rougeL']}
    
    for pred, ref in zip(predictions, references):
        scores = scorer.score(pred, ref)
        for metric in rouge_scores:
            rouge_scores[metric].append(scores[metric].fmeasure)
    
    # Calculate BLEU
    bleu_score = bleu.compute(predictions=predictions, references=[[r] for r in references])
    
    # Calculate exact matches
    exact_matches = [1 if p.strip() == r.strip() else 0 for p, r in zip(predictions, references)]
    
    return {
        'rouge1': np.mean(rouge_scores['rouge1']),
        'rouge2': np.mean(rouge_scores['rouge2']),
        'rougeL': np.mean(rouge_scores['rougeL']),
        'bleu': bleu_score['bleu'],
        'exact_match': np.mean(exact_matches)
    }

def run_evaluation(**context):
    """Main evaluation function"""
    logging.info("Starting model evaluation")
    
    # Load test data
    eval_data = load_test_data(**context)
    logging.info(f"Loaded {len(eval_data)} test examples")
    
    # Initialize model
    model = TextGenerationModel.from_pretrained("text-bison@001")
    
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
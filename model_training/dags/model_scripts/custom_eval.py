# from typing import Dict, Any, List, Optional
# from vertexai.preview.language_models import TextGenerationModel
# import numpy as np
# import logging
# from dataclasses import dataclass
# from vertexai.preview.evaluation import MetricProducer, MetricResult
# import json

# @dataclass
# class CustomMetricConfig:
#     """Configuration for custom metrics"""
#     relevance_prompt_template: str = """You are an expert evaluator. Rate how relevant the given answer is to the question on a scale of 0-1.
#     Focus only on the relevance of the information provided, ignoring any extra or tangential information.

#     Question: {question}
#     Answer: {answer}

#     Scoring Guide:
#     1.0: Perfect relevance - Answer directly addresses the question with no irrelevant information
#     0.8: High relevance - Answer mostly addresses the question with minimal irrelevant information
#     0.5: Moderate relevance - Answer partially addresses the question with some irrelevant information
#     0.2: Low relevance - Answer barely addresses the question with mostly irrelevant information
#     0.0: No relevance - Answer does not address the question at all

#     Provide your score as a single number between 0 and 1, followed by a brief explanation.
#     Score:"""

#     coverage_prompt_template: str = """You are an expert evaluator. Rate how completely the given answer covers all aspects of the question on a scale of 0-1.
#     Consider whether all parts of the question are addressed and the depth of coverage for each part.

#     Question: {question}
#     Answer: {answer}

#     First, break down the key elements that need to be covered:
#     1. List each component of the question that requires addressing
#     2. Check if each component is covered in the answer
#     3. Evaluate the depth of coverage for each component

#     Scoring Guide:
#     1.0: Complete coverage - All components thoroughly addressed
#     0.8: High coverage - Most components thoroughly addressed
#     0.5: Moderate coverage - Some components addressed or all addressed superficially
#     0.2: Low coverage - Few components addressed
#     0.0: No coverage - No components adequately addressed

#     Provide your score as a single number between 0 and 1, followed by a brief explanation.
#     Score:"""

# class AnswerRelevanceMetric(MetricProducer):
#     """Custom metric for measuring answer relevance"""
    
#     def __init__(self, 
#                  evaluation_model: TextGenerationModel,
#                  config: Optional[CustomMetricConfig] = None):
#         self.model = evaluation_model
#         self.config = config or CustomMetricConfig()
        
#     def _extract_score(self, response: str) -> float:
#         """Extract numerical score from model response"""
#         try:
#             # Find the first number in the response
#             import re
#             score_match = re.search(r'(\d*\.?\d+)', response)
#             if score_match:
#                 score = float(score_match.group(1))
#                 return min(max(score, 0.0), 1.0)  # Ensure score is between 0 and 1
#             return 0.0
#         except Exception as e:
#             logging.error(f"Error extracting score: {e}")
#             return 0.0

#     def evaluate_example(self, example: Dict[str, Any]) -> MetricResult:
#         """Evaluate a single example for answer relevance"""
#         try:
#             # Format the prompt
#             prompt = self.config.relevance_prompt_template.format(
#                 question=example["inputs"],
#                 answer=example["outputs"]
#             )
            
#             # Get evaluation from model
#             response = self.model.predict(prompt).text
#             score = self._extract_score(response)
            
#             return MetricResult(
#                 metric_name="answer_relevance",
#                 value=score,
#                 metadata={
#                     "explanation": response,
#                     "question": example["inputs"],
#                     "answer": example["outputs"]
#                 }
#             )
#         except Exception as e:
#             logging.error(f"Error evaluating relevance: {e}")
#             return MetricResult(
#                 metric_name="answer_relevance",
#                 value=0.0,
#                 metadata={"error": str(e)}
#             )

# class AnswerCoverageMetric(MetricProducer):
#     """Custom metric for measuring answer coverage"""
    
#     def __init__(self, 
#                  evaluation_model: TextGenerationModel,
#                  config: Optional[CustomMetricConfig] = None):
#         self.model = evaluation_model
#         self.config = config or CustomMetricConfig()

#     def evaluate_example(self, example: Dict[str, Any]) -> MetricResult:
#         """Evaluate a single example for answer coverage"""
#         try:
#             # Format the prompt
#             prompt = self.config.coverage_prompt_template.format(
#                 question=example["inputs"],
#                 answer=example["outputs"]
#             )
            
#             # Get evaluation from model
#             response = self.model.predict(prompt).text
#             score = self._extract_score(response)
            
#             return MetricResult(
#                 metric_name="answer_coverage",
#                 value=score,
#                 metadata={
#                     "explanation": response,
#                     "question": example["inputs"],
#                     "answer": example["outputs"]
#                 }
#             )
#         except Exception as e:
#             logging.error(f"Error evaluating coverage: {e}")
#             return MetricResult(
#                 metric_name="answer_coverage",
#                 value=0.0,
#                 metadata={"error": str(e)}
#             )

#     def _extract_score(self, response: str) -> float:
#         """Extract numerical score from model response"""
#         try:
#             import re
#             score_match = re.search(r'(\d*\.?\d+)', response)
#             if score_match:
#                 score = float(score_match.group(1))
#                 return min(max(score, 0.0), 1.0)
#             return 0.0
#         except Exception as e:
#             logging.error(f"Error extracting score: {e}")
#             return 0.0

# def aggregate_metrics(results: List[MetricResult]) -> Dict[str, Any]:
#     """Aggregate metrics across multiple examples"""
#     metrics = {
#         "answer_relevance": [],
#         "answer_coverage": []
#     }
    
#     for result in results:
#         if result.metric_name in metrics:
#             metrics[result.metric_name].append(result.value)
    
#     return {
#         "answer_relevance_mean": np.mean(metrics["answer_relevance"]),
#         "answer_relevance_std": np.std(metrics["answer_relevance"]),
#         "answer_coverage_mean": np.mean(metrics["answer_coverage"]),
#         "answer_coverage_std": np.std(metrics["answer_coverage"]),
#         "sample_size": len(metrics["answer_relevance"])
#     }
from vertexai.preview.evaluation import PointwiseMetric, PointwiseMetricPromptTemplate

# Define the prompt templates for relevance and coverage metrics
RELEVANCE_CRITERIA = {
    "answer_relevance": "Evaluate how relevant the given answer is to the question, focusing only on the relevance of the information provided.",
    "information_accuracy": "Assess whether the provided information directly addresses the question without tangential content.",
    "response_focus": "Check if the response stays focused on the core question being asked."
}

RELEVANCE_RUBRIC = {
    "5": "(Perfect relevance) Answer directly addresses the question with no irrelevant information",
    "4": "(High relevance) Answer mostly addresses the question with minimal irrelevant information",
    "3": "(Moderate relevance) Answer partially addresses the question with some irrelevant information",
    "2": "(Low relevance) Answer barely addresses the question with mostly irrelevant information",
    "1": "(No relevance) Answer does not address the question at all"
}

RELEVANCE_PROMPT_TEMPLATE = {
    "instruction": "You are an expert evaluator tasked with assessing how relevant the given answer is to the question. Focus only on the relevance of the information provided, ignoring any extra or tangential information.",
    "metric_definition": "You will assess answer relevance, which involves evaluating how directly and appropriately the response addresses the given question.",
    "criteria": RELEVANCE_CRITERIA,
    "rating_rubric": RELEVANCE_RUBRIC,
    "evaluation_steps": {
        "step1": "STEP 1: Analyze the core components of the question",
        "step2": "STEP 2: Identify the main points addressed in the answer",
        "step3": "STEP 3: Evaluate how well the answer's content aligns with the question's requirements"
    }
}

COVERAGE_CRITERIA = {
    "completeness": "Evaluate if all aspects of the question are addressed in the answer",
    "depth": "Assess the depth of coverage for each component of the question",
    "thoroughness": "Check if the response provides comprehensive information for all required elements"
}

COVERAGE_RUBRIC = {
    "5": "(Complete coverage) All components thoroughly addressed with detailed explanations",
    "4": "(High coverage) Most components thoroughly addressed with good detail",
    "3": "(Moderate coverage) Some components addressed or all addressed superficially",
    "2": "(Low coverage) Few components addressed with minimal detail",
    "1": "(No coverage) No components adequately addressed"
}

COVERAGE_PROMPT_TEMPLATE = {
    "instruction": "You are an expert evaluator tasked with assessing how completely the given answer covers all aspects of the question.",
    "metric_definition": "You will assess answer coverage, which involves evaluating whether all parts of the question are addressed and the depth of coverage for each part.",
    "criteria": COVERAGE_CRITERIA,
    "rating_rubric": COVERAGE_RUBRIC,
    "evaluation_steps": {
        "step1": "STEP 1: Break down the key elements that need to be covered",
        "step2": "STEP 2: Check if each component is covered in the answer",
        "step3": "STEP 3: Evaluate the depth of coverage for each component"
    }
}

# Create PointwiseMetric instances
answer_relevance_metric = PointwiseMetric(
    metric="answer-relevance",
    metric_prompt_template=PointwiseMetricPromptTemplate(
        criteria=RELEVANCE_PROMPT_TEMPLATE["criteria"],
        rating_rubric=RELEVANCE_PROMPT_TEMPLATE["rating_rubric"],
        instruction=RELEVANCE_PROMPT_TEMPLATE["instruction"],
        evaluation_steps=RELEVANCE_PROMPT_TEMPLATE["evaluation_steps"],
        metric_definition=RELEVANCE_PROMPT_TEMPLATE["metric_definition"]
    )
)

answer_coverage_metric = PointwiseMetric(
    metric="answer-coverage",
    metric_prompt_template=PointwiseMetricPromptTemplate(
        criteria=COVERAGE_PROMPT_TEMPLATE["criteria"],
        rating_rubric=COVERAGE_PROMPT_TEMPLATE["rating_rubric"],
        instruction=COVERAGE_PROMPT_TEMPLATE["instruction"],
        evaluation_steps=COVERAGE_PROMPT_TEMPLATE["evaluation_steps"],
        metric_definition=COVERAGE_PROMPT_TEMPLATE["metric_definition"]
    )
)

# List of custom metrics to be used in evaluation
CUSTOM_METRICS = [
    answer_relevance_metric,
    answer_coverage_metric
]
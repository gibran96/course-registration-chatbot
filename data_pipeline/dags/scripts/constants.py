import vertexai
from vertexai.generative_models import GenerativeModel

PROJECT_ID = "coursecompass"
TARGET_SAMPLE_COUNT = 100
GENERATED_SAMPLE_COUNT = 20

# Initialize Vertex AI
vertexai.init(project=PROJECT_ID, location="us-central1")
CLIENT_MODEL = GenerativeModel(model_name="gemini-1.5-flash-002")

LLM_PROMPT_TEMPLATE = """          
    Given the user question and the relevant information from the database, craft a concise and informative response:
    User Question:
    {query}
    Context:
    {content}
    The response should:
    1. Highlight the main topics and unique aspects of the course content.
    2. Summarize the instructor's teaching style and notable strengths or weaknesses.
    3. Clearly address potential benefits and challenges of the course, providing a straightforward recommendation as needed.
    Ensure the answer is direct, informative, and relevant to the user's question.
    """

QUERY_GENERATION_PROMPT = """Understand the following query provided by the user and generate 10 similar queries that can be phrased in different ways.

    Output the results in the following JSON format enclosed by triple backticks:
    ```json{{"queries": ["query_1","query_2",...]}}```

    User Query :
    {query}
    Generated Queries :
    """
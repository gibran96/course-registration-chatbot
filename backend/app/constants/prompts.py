QUERY_PROMPT = """
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

DEFAULT_RESPONSE = """
    I'm sorry, I couldn't find any relevant information to answer your question. 
    You might want to check the university's website, contact the registrar's office, or reach out to the specific department for more details. 
    Let me know if there's anything else I can assist you with!.
"""
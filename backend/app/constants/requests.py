from pydantic import BaseModel


class PredictionRequest(BaseModel):
    query: str
    session_id: str
    
class FeedbackRequest(BaseModel):
    session_id: str
    query_id: str
    feedback: str
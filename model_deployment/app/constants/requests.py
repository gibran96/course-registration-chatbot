from pydantic import BaseModel


class PredictionRequest(BaseModel):
    query: str
    session_id: str
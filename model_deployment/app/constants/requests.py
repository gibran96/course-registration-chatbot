from pydantic import BaseModel


class PredictionRequest(BaseModel):
    query: str
from fastapi import APIRouter, HTTPException
from app.services.llm_service import process_llm_request
from app.constants.requests import PredictionRequest
import logging

router = APIRouter()

@router.post("/predict")
async def get_response(request: PredictionRequest):    
    try:
        response = process_llm_request(request)
        return {"response": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

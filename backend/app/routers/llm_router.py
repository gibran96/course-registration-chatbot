from fastapi import APIRouter, HTTPException
from app.services.llm_service import process_llm_request
from app.constants.requests import PredictionRequest

router = APIRouter()

@router.post("/predict")
async def get_response(request: PredictionRequest):    
    try:
        response, query_id = process_llm_request(request)
        return {"query_id": query_id, "response": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

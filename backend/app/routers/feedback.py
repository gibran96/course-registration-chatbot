from fastapi import APIRouter, HTTPException
from app.services.feedback_service import save_feedback
from app.constants.requests import FeedbackRequest
import logging

router = APIRouter()

@router.post("/")
async def feedback(request: FeedbackRequest):    
    """
    Save feedback for a given query into the BigQuery table.

    Args:
        request: The request containing the session_id, query_id, and feedback

    Returns:
        Whether the request was successful or not

    Raises:
        HTTPException: If an error occurs during the insertion process.
    """
    try:
        response = save_feedback(request)
    except Exception as e:
        logging.error(f"Error saving feedback: {e}")
        raise HTTPException(status_code=500, detail="Error saving feedback")
    
    if not response:
        raise HTTPException(status_code=500, detail="Error saving feedback")
    
    return {"message": "Feedback saved successfully"}
from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def health_check():
    return {"message": "Hello World! The service is up and running."}

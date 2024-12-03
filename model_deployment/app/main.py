from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import health, llm_router

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router, prefix="/health", tags=["Health"])
app.include_router(llm_router.router, prefix="/llm", tags=["LLM"])   

from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_health_endpoint():
    response = client.get("/health/")
    assert response.status_code == 200
    assert response.json() == {"message": "Hello World! The service is up and running."}

def test_predict_endpoint_invalid_payload():
    response = client.post("/llm/predict", json={"wrong_key": "value"})
    assert response.status_code == 422  # Unprocessable Entity due to validation error

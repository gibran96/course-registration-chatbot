# Course Compass Backend

## Overview
The backend application is a FastAPI service that serves as the backend for the Course Compass project. It provides APIs for generating AI responses and handling user feedback. It is hosted on Google Cloud Run and uses Google Cloud Vertex AI for LLM interactions.

## Tech Stack
- **Web Framework**: FastAPI
- **Language Model**: Google Cloud Vertex AI
- **Database**: BigQuery
- **Programming Language**: Python 3.10

## Project Structure
```
backend/
├── Dockerfile                  # Docker configuration for containerizing the application
├── requirements.txt            # Python dependencies for the project
├── app/
│   ├── main.py                 # Main FastAPI application entry point
│   ├── constants/
│   │   ├── bq_queries.py       # BigQuery SQL queries for data retrieval and manipulation
│   │   ├── prompts.py          # Prompt templates for LLM interactions
│   │   └── requests.py         # Pydantic request models for API endpoints
│   ├── routers/
│   │   ├── health.py           # Health check endpoint for service status
│   │   ├── llm_router.py       # Router for LLM prediction endpoint
│   │   └── feedback.py         # Router for handling user feedback
│   ├── services/
│   │   ├── llm_service.py      # Business logic for LLM request processing
│   │   └── feedback_service.py # Business logic for saving user feedback
│   └── utils/
│       ├── bq_utils.py         # Utility functions for BigQuery interactions
│       ├── data_utils.py       # General data processing utilities
│       └── llm_utils.py        # Utility functions for LLM interactions (e.g., exponential backoff)
├── notebooks/
│   └── Drift Detection.ipynb   # Jupyter notebook for model drift analysis
└── tests/
    └── test_main.py            # Unit tests for the application
```

## Detailed File Descriptions

### Main Application Files
- `main.py`: Initializes the FastAPI application and sets up CORS middleware
- `Dockerfile`: Defines the Docker container configuration for deployment
- `requirements.txt`: Lists all Python package dependencies

### Constants
- `bq_queries.py`: Contains predefined SQL queries for:
  - Semantic similarity search
  - Session data retrieval
  - Feedback updates
- `prompts.py`: Stores prompt templates for guiding LLM responses
- `requests.py`: Defines Pydantic models for API request validation

### Routers
- `health.py`: Provides a simple health check endpoint to verify service status
- `llm_router.py`: Handles LLM prediction requests and routes them to the appropriate service
- `feedback.py`: Manages the endpoint for saving user feedback

### Services
- `llm_service.py`: Manages the core logic for:
  - Fetching context
  - Generating LLM responses
  - Tracking sessions
  - Inserting interaction data into BigQuery
- `feedback_service.py`: Handles the saving of user feedback to the database

### Utilities
- `bq_utils.py`: Provides functions for:
  - Fetching context from BigQuery
  - Inserting data into BigQuery
  - Checking existing sessions
- `data_utils.py`: Offers data processing utilities like removing punctuation
- `llm_utils.py`: Implements utility functions such as:
  - Exponential backoff for API calls
  - LLM response generation with safety settings

### Additional Components
- `tests/test_main.py`: Contains unit tests to ensure application reliability

## Key Features
- **Semantic Search**: Uses vector embeddings to find relevant course information
- **Conversational AI**: Provides context-aware responses about courses
- **Feedback Mechanism**: Allows users to provide feedback on chatbot responses
- **Session Tracking**: Maintains conversation context across interactions

## Environment Variables
Required environment variables:
- `PROJECT_ID`: Google Cloud Project ID
- `LOCATION`: Vertex AI location
- `ENDPOINT_ID`: Vertex AI model endpoint
- `DATASET_ID`: BigQuery dataset ID
- `USER_TABLE_NAME`: BigQuery table for user interactions

## Local Development Setup
1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Set up Google Cloud credentials
4. Run the application:
   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port 8080
   ```

## Docker Deployment
```bash
docker build -t course-compass-backend .
docker run -p 8080:8080 course-compass-backend
```

## Testing
Run tests using:
```bash
pytest tests/
```

## Deployment on Google Cloud Run

### Infrastructure
- **Hosting Platform**: Google Cloud Run
- **Container Registry**: Google Artifact Registry
- **Continuous Integration/Continuous Deployment (CI/CD)**: GitHub Actions

### Deployment Workflow
The application uses a GitHub Actions workflow for automated deployment triggered on pushes to the `main` branch. The workflow follows these key steps:

1. **Code Checkout**: Retrieves the latest code from the repository
2. **Google Cloud Authentication**: 
   - Authenticates with Google Cloud using a service account
   - Configures Docker to interact with Artifact Registry
3. **Docker Image Build**:
   - Builds a Docker image for the backend
   - Tags the image with the Artifact Registry path
4. **Image Deployment**:
   - Pushes the Docker image to Google Artifact Registry
   - Deploys the image to Google Cloud Run

### Deployment Configuration
- **Region**: us-east1
- **Platform**: Cloud Run (managed)
- **Scaling**:
  - Maximum Instances: 10
  - Automatic scaling based on traffic
- **Access**: Publicly accessible (unauthenticated)

### Environment Variables
The deployment configures the following environment variables:
- `PROJECT_ID`: Google Cloud Project ID
- `ENDPOINT_ID`: Vertex AI model endpoint
- `LOCATION`: Google Cloud location
- `DATASET_ID`: BigQuery dataset identifier
- `USER_TABLE_NAME`: BigQuery table for user interactions

### Continuous Deployment Triggers
The GitHub Actions workflow is triggered when changes are pushed to the `main` branch, specifically when modifications occur in:
- `backend/` directory
- `.github/workflows/backend-deploy.yaml` file

### Security Considerations
- Uses a dedicated service account for deployment
- Stores sensitive credentials as GitHub Secrets
- Limits maximum instances to control potential costs
- Allows unauthenticated access for public use

### Monitoring and Logging
- Inherent logging and monitoring provided by Google Cloud Run
- Detailed request logs and performance metrics available in Google Cloud Console

## Deployment Prerequisites
To set up similar deployment:
1. Create a Google Cloud Platform (GCP) project
2. Enable Cloud Run and Artifact Registry APIs
3. Create a service account with necessary permissions
4. Configure GitHub Secrets and Variables:
   - `GCP_SERVICE_ACCOUNT_KEY`: Service account JSON key
   - `GCP_SERVICE_ACCOUNT_ID`: Service account email
   - `GCP_PROJECT_ID`: GCP Project ID
   - `GCP_LOCATION`: Deployment region
   - Other project-specific environment variables

## Manual Deployment
For manual deployment, you can use the gcloud CLI:
```bash
gcloud run deploy course-compass \
  --image us-east1-docker.pkg.dev/coursecompass/course-compass-backend/course-compass \
  --region us-east1 \
  --platform managed \
  --allow-unauthenticated \
  --update-env-vars PROJECT_ID=$PROJECT_ID \
  --update-env-vars ENDPOINT_ID=$ENDPOINT_ID \
  --update-env-vars LOCATION=$LOCATION \
  --service-account $GCP_SERVICE_ACCOUNT_ID
```

## Endpoints
- `/health/`: Health check endpoint
- `/llm/predict`: Generate AI responses
- `/feedback/`: Submit user feedback

## Logging
The application uses Python's logging module to track events and errors.

## Safety and Moderation
- Implements safety settings for generated content
- Uses exponential backoff for API calls to handle temporary failures
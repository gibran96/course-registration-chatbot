# Use an official Python image
FROM python:3.10-slim

# Set working directory
WORKDIR /app


# Install dependencies
RUN pip install --no-cache-dir streamlit requests

# Copy application code
COPY . .

# Expose port for FastAPI
EXPOSE 8501

# Command to run the FastAPI app
CMD ["streamlit", "run", "app.py"]


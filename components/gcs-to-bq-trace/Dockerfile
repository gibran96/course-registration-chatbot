# Use the Google Cloud Functions Python base image
FROM gcr.io/google.com/cloudsdktool/cloud-sdk:slim

# Set environment variables
ENV FUNCTION_TARGET=load_trace_data_into_bigquery
ENV BQ_TABLE=my_project.my_dataset.my_table  # Change to your project, dataset, and table

# Copy code to /function directory
WORKDIR /function
COPY . .

# Install dependencies
RUN apt-get update && apt-get install -y python3 python3-pip && pip3 install google-cloud-bigquery google-cloud-storage

# Entrypoint to run the function
CMD ["functions-framework", "--target", "load_trace_data_into_bigquery", "--port", "8080"]

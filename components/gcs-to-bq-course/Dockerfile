FROM gcr.io/google.com/cloudsdktool/cloud-sdk:slim

ENV FUNCTION_TARGET=load_course_data_into_bigquery
ENV BQ_TABLE=my_project.my_dataset.my_table  # Change to your project, dataset, and table

WORKDIR /function
COPY . .

RUN apt-get update && apt-get install -y python3 python3-pip && pip3 install google-cloud-bigquery google-cloud-storage

CMD ["functions-framework", "--target", "load_course_data_into_bigquery", "--port", "8080"]

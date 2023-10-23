FROM python:3.8-slim-buster

# Set environment variables
ENV AIRFLOW_HOME=/usr/local/airflowx

# (Optional) Set a working directory inside the container
WORKDIR /app

# Install dependencies
RUN apt-get update -y && \
    apt-get install -y gcc && \
    pip install --upgrade pip && \
    pip install apache-airflow[postgres,celery] && \
    pip install kaggle && \
    pip install pyspark && \
    pip install azure-storage-blob && \
    pip install azure-storage-file-datalake && \
    pip install azure-keyvault-secrets && \
    pip install microsoft azure-monitor-ingestion && \
    pip install loguru && \

# Create Airflow user and directories
RUN useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow && \
    mkdir -p ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/logs

# Switch to airflow user
USER airflow

# Initialize the database
RUN airflow db init

EXPOSE 8080

CMD ["airflow", "webserver", "--port", "8080"]

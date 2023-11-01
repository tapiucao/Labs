FROM python:3.11-slim-buster

# Set environment variables
ENV AIRFLOW_HOME=/usr/local/airflow

# (Optional) If you have requirements for your Python app, add them
COPY requirements.txt .

# Install dependencies
RUN apt-get update -y && \
    apt-get install -y gcc && \
    pip install --upgrade pip && \
    pip install -r requirements.txt && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create Airflow user and directories and give proper permissions
RUN useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow && \
    mkdir -p ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/logs && \
    chmod -R 755 /usr/local/airflow/logs && \
    chmod -R 755 /usr/local/airflow/dags 

EXPOSE 8080


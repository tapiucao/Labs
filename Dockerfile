FROM python:3.11-slim-buster

# Set environment variables
ENV AIRFLOW_HOME=/usr/local/airflow

COPY importdata.py .

# (Optional) If you have requirements for your Python app, add them
COPY requirements.txt .

# Install dependencies
RUN apt-get update -y && \
    apt-get install -y gcc && \
    pip install --upgrade pip && \
    pip install -r requirements.txt && \
    pip install apache-airflow[azure]

# Create Airflow user and directories and give proper permissions
RUN useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow && \
    mkdir -p ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/logs && \
    chmod -R 777 /usr/local/airflow/logs && \
    chmod -R 777 /usr/local/airflow/dags
    
# Switch to airflow user
USER airflow

# Initialize the database
RUN airflow db init

EXPOSE 8080

CMD ["airflow", "webserver", "--port", "8080"]

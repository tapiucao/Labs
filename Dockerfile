FROM apache/airflow:2.7.3-python3.11

COPY requirements.txt /requirements.txt

COPY /.kaggle  /home/airflow/.kaggle
COPY /.kaggle ~/.kaggle

# Install additional dependencies if needed (example shown)
RUN pip install --upgrade pip &&\
    pip install --no-cache-dir -r /requirements.txt &&\
    pip install virtualenv
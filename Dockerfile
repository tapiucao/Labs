# Use the official Airflow image as a base
FROM apache/airflow:slim-2.7.2-python3.11

COPY requirements.txt /requirements.txt

# Install additional dependencies if needed (example shown)
RUN pip install --upgrade pip &&\
    pip install --no-cache-dir -r /requirements.txt &&\
    pip install mysql-connector-python &&\
    pip install virtualenv 

RUN chmod -R 775 /opt/airflow

# Copy the entrypoint script
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

# Switch to root to change file permissions
USER root

# Set the script to be executable
RUN chmod +x /usr/local/bin/entrypoint.sh

# Switch back to the default user
USER airflow

# Set the entrypoint
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
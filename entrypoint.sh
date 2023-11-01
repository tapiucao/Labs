#!/bin/bash
set -e

# Initialize the database
airflow db init

# Create default user
airflow users create --role Admin --username admin --email tapiucao@gmail.com --firstname admin --lastname admin --password admin

# Execute the command provided as arguments to the entrypoint
exec "$@"

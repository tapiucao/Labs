#!/bin/bash

# Script to wait for MySQL to be ready and perform Airflow DB migration

# Set maximum number of attempts to connect to MySQL
MAX_ATTEMPTS=30
ATTEMPT_NUM=1

echo "Waiting for MySQL to become available..."

# Loop until we're able to connect to MySQL or reach the MAX_ATTEMPTS
until mysqladmin ping -h"mysql" -u"airflow" -p"airflow_pass" --silent; do
    if [ ${ATTEMPT_NUM} -eq ${MAX_ATTEMPTS} ]; then
        echo "Attempted to connect to MySQL ${MAX_ATTEMPTS} times, but failed."
        echo "Exiting."
        exit 1
    fi

    printf "."
    sleep 5
    ATTEMPT_NUM=$((ATTEMPT_NUM+1))
done

echo "MySQL is up and accepting connections."

# Proceed with the Airflow DB migration
airflow db migrate

# Start Airflow component based on the argument provided to the script
if [ "$1" = "webserver" ]; then
    exec airflow webserver
elif [ "$1" = "scheduler" ]; then
    exec airflow scheduler
else
    exec "$@"
fi

# Start Airflow components, e.g., webserver or scheduler based on the provided CMD to the Docker container
exec "$@"

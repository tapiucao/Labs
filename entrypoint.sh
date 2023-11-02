#!/bin/bash

# Wait for the database service to be up
# This could be a loop that waits for MySQL to respond

# Run db init
airflow db migrate

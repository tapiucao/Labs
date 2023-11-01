import os
import kaggle
from pyspark.sql import SparkSession
from io import StringIO
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from loguru import logger
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


def fetch_kaggle_dataset_as_dataframe(dataset_name, file_name):
    """
    Fetch a specified Kaggle dataset file and return it as a Pandas DataFrame.

    Args:
    - dataset_name (str): The identifier for the dataset in format "USERNAME/DATASET".
    - file_name (str): The specific file within the dataset.

    Returns:
    - pd.DataFrame: DataFrame containing the dataset's data.
    """

    # Create a temporary directory for the Kaggle dataset
    download_dir = "./temp_kaggle_download"
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
        logger.info(f"Created directory: {download_dir}")

    # Authenticate and download the dataset
    logger.info(f"Authenticating and downloading dataset: {dataset_name}")
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(dataset_name, path=download_dir, unzip=True)

    # Path to the desired file within the dataset
    file_path = os.path.join(download_dir, file_name)

    if not os.path.exists(file_path):
        logger.error(f"File {file_name} not found in downloaded dataset.")
        return None

    # Load the file into a Pandas DataFrame
    logger.info(f"Reading file {file_name} into DataFrame.")
    df = pd.read_csv(file_path)

    # Clean up (delete the temporary dataset directory)
    logger.info("Cleaning up temporary files.")
    os.remove(file_path)
    os.rmdir(download_dir)

    return df


# If you want to log to a file as well, you can configure logger like:
# logger.add("filename.log")


def group_and_count_ordered(df_pandas):
    """
    Receives a Pandas DataFrame, uses Spark to perform row count grouped by 'town',
    orders the result by count, and returns the result as a Pandas DataFrame.

    Args:
    - df_pandas (pd.DataFrame): Input Pandas DataFrame.

    Returns:
    - pd.DataFrame: Resultant DataFrame with counts per town ordered by count.
    """

    logger.info("Initializing Spark session.")
    # Initialize Spark session
    spark = SparkSession.builder.appName("GroupByTownOrdered").getOrCreate()

    # Convert Pandas DataFrame to Spark DataFrame
    logger.info("Converting Pandas DataFrame to Spark DataFrame.")
    df_spark = spark.createDataFrame(df_pandas)

    # Group by 'town', count rows, and order by count
    logger.info("Grouping by 'town', counting rows, and ordering by count.")
    grouped_df_spark = (
        df_spark.groupBy("town").count().orderBy("count", ascending=False)
    )

    # Convert the result back to Pandas DataFrame
    logger.info("Converting the result back to Pandas DataFrame.")
    result_df_pandas = grouped_df_spark.toPandas()

    # Stop the Spark session
    logger.info("Stopping the Spark session.")
    spark.stop()

    return result_df_pandas


def write_df_to_adls2(account_name, account_key, file_system_name, file_path, df):
    """
    Write a pandas DataFrame to Azure Data Lake Storage Gen2 as a CSV file.

    Parameters:
    - account_name (str): Azure storage account name
    - account_key (str): Azure storage account key
    - file_system_name (str): Name of the file system (container) in ADLS Gen2
    - file_path (str): Path of the file inside the file system, including its name
    - df (pd.DataFrame): DataFrame to be written
    """
    logger.info(
        f"Writing DataFrame to Azure Data Lake Storage Gen2 at {file_path} in {file_system_name}."
    )

    # Create a Data Lake service client using account name and key
    logger.debug("Creating Data Lake service client.")
    service_client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key,
    )

    # Get the file system client for the specified file system
    logger.debug(f"Getting file system client for: {file_system_name}.")
    file_system_client = service_client.get_file_system_client(file_system_name)

    # Get the data lake file client for the specified file path
    logger.debug(f"Getting file client for: {file_path}.")
    file_client = file_system_client.get_file_client(file_path)

    # Convert the dataframe to CSV format and get the content in bytes
    logger.debug("Converting DataFrame to CSV format.")
    csv_content = StringIO()
    df.to_csv(csv_content, index=False)
    csv_bytes = csv_content.getvalue().encode("utf-8")

    # Upload the content to the file
    logger.info("Uploading CSV content to Azure Data Lake Storage Gen2.")
    file_client.upload_data(csv_bytes, overwrite=True)
    logger.success(f"Successfully uploaded DataFrame to {file_path}.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    "example_dag",
    default_args=default_args,
    description="An example DAG",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 24),
    catchup=False,
) as dag:
    start_task = DummyOperator(task_id="start_task")

    fetch_data_task = PythonOperator(
        task_id="fetch_data_task",
        python_callable=fetch_kaggle_dataset_as_dataframe,
        op_kwargs={
            "dataset_name": "anoopjohny/real-estate-sales-2001-2020-state-of-connecticut",
            "file_name": "Real_Estate_Sales_2001-2020_GL.csv",
        },
    )

    process_data_task = PythonOperator(
        task_id="process_data_task",
        python_callable=group_and_count_ordered,
    )

    store_data_task = PythonOperator(
        task_id="store_data_task",
        python_callable=write_df_to_adls2,
        op_kwargs={
            "account_name": "montrealadls",
            "account_key": "dWksQ33gDM56isvYdBv0U/lrOSwK5QQPfLRKCKJagYBhc0pR4UIb2GpPj+tvMT6oFUX24J/fi8lv+AStybQh1g==",
            "file_system_name": "montrealfilesystem",
            "file_path": "test_airflow.csv",
        },
    )

    end_task = DummyOperator(task_id="end_task")

    start_task >> fetch_data_task >> process_data_task >> store_data_task >> end_task

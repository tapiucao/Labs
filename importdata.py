import os
import kaggle
from pyspark.sql import SparkSession
from io import StringIO
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from loguru import logger 


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

    # Authenticate and download the dataset
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(dataset_name, path=download_dir, unzip=True)

    # Path to the desired file within the dataset
    file_path = os.path.join(download_dir, file_name)
    
    # Load the file into a Pandas DataFrame
    df = pd.read_csv(file_path)
    
    # Clean up (delete the temporary dataset directory)
    os.remove(file_path)
    os.rmdir(download_dir)
    
    return df


def group_and_count_ordered(df_pandas):
    """
    Receives a Pandas DataFrame, uses Spark to perform row count grouped by 'town', 
    orders the result by count, and returns the result as a Pandas DataFrame.

    Args:
    - df_pandas (pd.DataFrame): Input Pandas DataFrame.

    Returns:
    - pd.DataFrame: Resultant DataFrame with counts per town ordered by count.
    """

    # Initialize Spark session
    spark = SparkSession.builder.appName("GroupByTownOrdered").getOrCreate()

    # Convert Pandas DataFrame to Spark DataFrame
    df_spark = spark.createDataFrame(df_pandas)

    # Group by 'town', count rows, and order by count
    grouped_df_spark = df_spark.groupBy("town").count().orderBy("count", ascending=False)

    # Convert the result back to Pandas DataFrame
    result_df_pandas = grouped_df_spark.toPandas()

    # Stop the Spark session
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
    # Create a Data Lake service client using account name and key
    service_client = DataLakeServiceClient(account_url=f"https://{account_name}.dfs.core.windows.net",
                                           credential=account_key)

    # Get the file system client for the specified file system
    file_system_client = service_client.get_file_system_client(file_system_name)

    # Get the data lake file client for the specified file path
    file_client = file_system_client.get_file_client(file_path)

    # Convert the dataframe to CSV format and get the content in bytes
    csv_content = StringIO()
    df.to_csv(csv_content, index=False)
    csv_bytes = csv_content.getvalue().encode('utf-8')

    # Upload the content to the file
    file_client.upload_data(csv_bytes, overwrite=True)


import os
import kaggle
from pyspark.sql import SparkSession

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

def save_dataframe_to_adls2(df, account_name, account_key, filesystem_name, file_path):
    """
    Saves a Pandas DataFrame to Azure Data Lake Storage Gen2.

    Args:
    - df (pd.DataFrame): The DataFrame to save.
    - account_name (str): The ADLS Gen2 account name.
    - account_key (str): The ADLS Gen2 account key.
    - filesystem_name (str): The name of the filesystem (equivalent to a container in Blob storage).
    - file_path (str): The path where the file should be saved, including the filename (e.g., "folder/data.csv").

    Returns:
    None
    """

    # Convert DataFrame to CSV string
    csv_data = df.to_csv(index=False)

    # Establish a connection to ADLS Gen2
    service_client = DataLakeServiceClient(account_url=f"https://{account_name}.dfs.core.windows.net", 
                                           credential=account_key)

    # Get the filesystem client
    filesystem_client = service_client.get_file_system_client(filesystem_name)

    # Get the file client and upload data
    file_client = filesystem_client.get_file_client(file_path)
    file_client.upload_data(csv_data, overwrite=True)


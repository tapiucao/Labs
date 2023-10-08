import os
import kaggle
from pyspark.sql import SparkSession

def download_dataset(dataset_name, path_to_store):
    """
    Downloads a Kaggle dataset.
    
    Args:
    - dataset_name (str): The identifier for the dataset in format "USERNAME/DATASET".
    - path_to_store (str): Directory path to store the downloaded dataset.
    """
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(dataset_name, path=path_to_store, unzip=True)

def read_dataset_with_spark(spark, file_path):
    """
    Reads a dataset into a Spark DataFrame.
    
    Args:
    - spark (SparkSession): Spark session instance.
    - file_path (str): Path to the dataset file.
    
    Returns:
    - Spark DataFrame
    """
    return spark.read.csv(file_path, header=True, inferSchema=True)

# Initialize Spark
spark = SparkSession.builder \
    .appName("KaggleDataProcessing") \
    .getOrCreate()

# Example usage:
dataset = "cisautomotiveapi/large-car-dataset"  # This is just an example dataset
download_path = "./"
dataset_file = "CIS_Automotive_Kaggle_Sample.csv"  # Replace with the desired file in the dataset
download_dataset(dataset, download_path)

df = read_dataset_with_spark(spark, os.path.join(download_path, dataset_file))

# Example transformation: Counting rows in the DataFrame
row_count = df.count()
print(f"Number of rows in the dataset: {row_count}")

# Stop the Spark session
spark.stop()

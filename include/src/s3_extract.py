import pandas as pd
import io
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from .postgres_loader import Postgres
import logging

logger = logging.getLogger(__name__)

def process_s3_file(file_key, bucket_name):
    """
    Fetch a file from S3, process it into a DataFrame, and load it into PostgreSQL.
    """
    logger.info(f"Fetching file: {file_key}")
    try:
        s3_hook = S3Hook('s3_connection')
        file_obj = s3_hook.get_key(key=file_key, bucket_name=bucket_name)
        
        if file_obj is None:
            logger.error(f"File {file_key} not found in bucket {bucket_name}.")
            return

        # Read the file content into a DataFrame
        df = pd.read_csv(io.BytesIO(file_obj.get()['Body'].read()))
        logger.info(f"Data from {file_key}:\n{df.head()}")

        # Add file name and date to the DataFrame
        df['file_name'] = file_key.split('/')[-1]
        df['date'] = pd.to_datetime('now')
        logger.info(f"Data loaded to Postgres for file: {file_key}")

        # Initialize Postgres loader and load data into Postgres
        postgres = Postgres(conn_id='postgres_connection')
        tables = {file_key.split('/')[-1].replace('.csv', '').lower(): df}
        postgres.load_to_postgres(tables)

    except Exception as e:
        logger.error(f"Error processing file {file_key}: {str(e)}")

def list_and_process_files(bucket_name, prefix):
    """
    List files in an S3 bucket under a specified prefix and process each file.
    """
    logger.info(f"Listing files in S3 bucket {bucket_name} with prefix {prefix}")
    try:
        s3_hook = S3Hook('s3_connection')
        files = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)

        if not files:
            logger.warning(f"No files found in S3 bucket {bucket_name} with prefix {prefix}")
            return
        
        # Process each file
        for file_key in files:
            if not file_key.endswith('.csv'):
                logger.info(f"Skipping non-CSV file: {file_key}")
                continue
            process_s3_file(file_key, bucket_name)
    
    except Exception as e:
        logger.error(f"Error listing files in S3: {str(e)}")
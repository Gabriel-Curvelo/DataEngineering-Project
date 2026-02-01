# This script aims to transform data into a columnar storage format (Parquet),
# and partition it by brewery location.

from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col, lit
import os
import json
import boto3
from datetime import datetime
import logging

# Path to the credentials file
CREDENTIALS_PATH = os.getenv("MINIO_KEYS_FILE", "/usr/local/airflow/include/keys/minio_credentials.json")

# Load keys
def load_credentials(path=CREDENTIALS_PATH):
    with open(path, "r") as f:
        return json.load(f)

# Get the latest JSON file from the bucket
def get_latest_json_file(s3, bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.json')]
    if not files:
        raise FileNotFoundError("No JSON files found in the bucket.")
    latest_file = sorted(files)[-1]
    logging.info(f"Latest file found: {latest_file}")
    return latest_file

# Write the data to Parquet, partitioned by country
def main():
    # Load MinIO credentials
    creds = load_credentials()
    endpoint = creds["endpoint"]
    access_key = creds["access_key"]
    secret_key = creds["secret_key"]
    bucket_bronze = creds["bucket_bronze"]
    bucket_silver = creds["bucket_silver"]
    prefix = creds["prefix"]

    # Create MinIO client via boto3
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    latest_file_key = get_latest_json_file(s3, bucket_bronze, prefix)
    response = s3.get_object(Bucket=bucket_bronze, Key=latest_file_key)
    content = response['Body'].read().decode('utf-8')

    temp_input_path = "/tmp/latest_brewery.json"
    with open(temp_input_path, "w") as f:
        f.write(content)

    # Start Spark session
    spark = (
        SparkSession.builder
        .appName("silverlayer")
        .master("local[*]")
        .getOrCreate()
    )

    # Read JSON files and create the DataFrame
    df = spark.read.option("multiline", "true").option("encoding", "UTF-8").json(temp_input_path)

    # Remove leading/trailing whitespaces from string columns
    df = df.select([trim(col(c)).alias(c) if dtype == "string" else col(c) for c, dtype in df.dtypes])

    # Add the execution_time column with the current timestamp
    execution_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df = df.withColumn("execution_time", lit(execution_time))

    output_path = f"/tmp/silver_output"

    df.write.partitionBy("country").parquet(output_path, mode="overwrite")

    # Upload the partitioned files to MinIO
    for root, _, files in os.walk(output_path):
        for file in files:
            full_path = os.path.join(root, file)
            relative_path = os.path.relpath(full_path, output_path)
            key = os.path.join(prefix, relative_path).replace("\\", "/")
            with open(full_path, "rb") as f:
                s3.put_object(
                    Bucket=bucket_silver,
                    Key=key,
                    Body=f.read(),
                    ContentType="application/octet-stream"
                )
            logging.info(f"File saved to: s3://{bucket_silver}/{key}")

if __name__ == "__main__":
    main()

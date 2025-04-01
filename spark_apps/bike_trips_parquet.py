import os
import requests
import zipfile
import logging
import math
import gc
import glob
import shutil
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, 
                               IntegerType, FloatType, TimestampType)
from pyspark.sql.functions import when,col, to_date, to_timestamp,regexp_replace

from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
file_handler = logging.FileHandler("/opt/spark/logs/spark_job_trips.log")
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# Constants
YEARS = range(2014, 2020)  # For testing, using only 2014; extend to 2023 as needed
BASE_ZIP_URL = "https://s3.amazonaws.com/tripdata/{}-citibike-tripdata.zip"
ZIP_DIR = "zip_files"
EXTRACT_DIR = "citibike_tripdata"

# BigQuery Configuration
BQ_PROJECT = "conicle-ai"
BQ_DATASET = "Recommend"
BQ_TABLE = "citibike_trips"
BQ_TABLE_FULL = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"


BQ_EXTERNAL_TABLE = "citibike_trips_external"  # External table name
BQ_EXTERNAL_TABLE_FULL = f"`{BQ_PROJECT}.{BQ_DATASET}.{BQ_EXTERNAL_TABLE}`"

# GCS Bucket for Parquet files (for external table in BigQuery)
GCS_BUCKET = "citi_bike_trips_parquet"


# Load Google Cloud credentials
service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/opt/gcp/google_credentials.json")
credentials = service_account.Credentials.from_service_account_file(service_account_path)

# Initialize BigQuery client
client = bigquery.Client(credentials=credentials, project=BQ_PROJECT)

def download_zip(year):
    """Download the zip file for a given year if not already downloaded."""
    if not os.path.exists(ZIP_DIR):
        os.makedirs(ZIP_DIR)
    zip_filename = os.path.join(ZIP_DIR, f"{year}-citibike-tripdata.zip")
    if not os.path.exists(zip_filename):
        url = BASE_ZIP_URL.format(year)
        logger.info(f"Downloading {zip_filename} from {url} ...")
        response = requests.get(url, stream=True)
        with open(zip_filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        logger.info("Download complete.")
    else:
        logger.info(f"Zip file for {year} already exists, skipping download.")
    return zip_filename

def unzip_file(year, zip_filename):
    """
    Unzip the downloaded file for a given year.
    The extracted folder structure will be:
        citibike_tripdata/{year}/<extracted_folder(s)>
    """
    extract_path = os.path.join(EXTRACT_DIR, str(year))
    if not os.path.exists(extract_path):
        logger.info(f"Extracting {zip_filename} into {extract_path} ...")
        with zipfile.ZipFile(zip_filename, "r") as zip_ref:
            zip_ref.extractall(extract_path)
        logger.info("Extraction complete.")
    else:
        logger.info(f"Data for year {year} already extracted, skipping extraction.")
    return extract_path


def create_gcs_bucket(bucket_name):
    """Create a GCS bucket if it does not already exist."""
    storage_client = storage.Client(credentials=credentials, project=BQ_PROJECT)
    try:
        bucket = storage_client.get_bucket(bucket_name)
        logger.info(f"GCS bucket '{bucket_name}' already exists.")
    except Exception:
        logger.info(f"GCS bucket '{bucket_name}' not found, creating bucket...")
        bucket = storage_client.bucket(bucket_name)
        bucket.location = "US"  # Set to your desired location
        bucket = storage_client.create_bucket(bucket)
        logger.info(f"GCS bucket '{bucket_name}' created.")


def process_and_upload_year(year):
    """
    1. Reads each monthly CSV for the given year from local disk.
    2. Writes the DataFrame as Parquet files locally in /spark_outputs/<year_month>/,
       preserving multiple part files.
    3. Renames each part file to part-XXXX-year_month.parquet.
    4. Uploads each renamed part file to the GCS bucket.
    5. Skips the _SUCCESS file.
    6. Deletes the local directory to free memory.
    """
    try:
        logger.info(f"Starting processing for year: {year}")

        # Initialize Spark session
        spark = SparkSession.builder \
            .master("local[*]") \
            .appName(f"Citibike_Trips_ETL_{year}") \
            .config("spark.jars", "/opt/spark/jars/spark-3.5-bigquery-0.42.1.jar,/opt/spark/jars/gcs-connector-hadoop3-latest.jar") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .getOrCreate()

        # Configure Hadoop for GCS if needed
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

        # Set up the Google Cloud Storage client
        storage_client = storage.Client(credentials=credentials, project=BQ_PROJECT)
        bucket = storage_client.bucket(GCS_BUCKET)

        # Define the schema for the Citibike trips data.
        schema = StructType([
            StructField("tripduration", IntegerType(), True),
            StructField("starttime", StringType(), True),
            StructField("stoptime", StringType(), True),
            StructField("start_station_id", IntegerType(), True),
            StructField("start_station_name", StringType(), True),
            StructField("start_station_latitude", FloatType(), True),
            StructField("start_station_longitude", FloatType(), True),
            StructField("end_station_id", IntegerType(), True),
            StructField("end_station_name", StringType(), True),
            StructField("end_station_latitude", FloatType(), True),
            StructField("end_station_longitude", FloatType(), True),
            StructField("bikeid", IntegerType(), True),
            StructField("usertype", StringType(), True),
            StructField("birth_year", IntegerType(), True),
            StructField("gender", StringType(), True),
        ])

        consistent_mapping = {
            "tripduration": "tripduration",
            "trip_duration": "tripduration",
            "starttime": "starttime",
            "start_time": "starttime",
            "stoptime": "stoptime",
            "stop_time": "stoptime",
            "start_station_id": "start_station_id",
            "start_station_name": "start_station_name",
            "start_station_latitude": "start_station_latitude",
            "start_station_longitude": "start_station_longitude",
            "end_station_id": "end_station_id",
            "end_station_name": "end_station_name",
            "end_station_latitude": "end_station_latitude",
            "end_station_longitude": "end_station_longitude",
            "bikeid": "bikeid",
            "bike_id": "bikeid",
            "usertype": "usertype",
            "user_type": "usertype",
            "birth_year": "birth_year",
            "gender": "gender"
        }

        extract_path = os.path.join(EXTRACT_DIR, str(year))
        if not os.path.exists(extract_path):
            logger.warning(f"No extracted data for {year}. Skipping.")
            return

        logger.info(f"Contents of {extract_path}: {os.listdir(extract_path)}")

        # Group CSV files by year-month (e.g., "2014_06")
        month_groups = {}
        for folder_name in sorted(os.listdir(extract_path)):
            if folder_name.startswith("__"):
                continue
            folder_path = os.path.join(extract_path, folder_name)
            if not os.path.isdir(folder_path):
                continue
            logger.info(f"Contents of {folder_path}: {os.listdir(folder_path)}")
            logger.info(f"Processing folder: {folder_path}")
            csv_files = glob.glob(os.path.join(folder_path, "**/*.csv"), recursive=True)
            csv_files += glob.glob(os.path.join(folder_path, "**/*.CSV"), recursive=True)
            if not csv_files:
                logger.info(f"No CSV files found in folder: {folder_path}")
                continue
            for csv_file in csv_files:
                basename = os.path.basename(csv_file)
                m = re.match(r'(\d{4})(\d{2}).*', basename)
                if m:
                    year_month = f"{m.group(1)}_{m.group(2)}"
                    month_groups.setdefault(year_month, []).append(csv_file)
                else:
                    logger.warning(f"Filename {basename} does not match expected pattern. Skipping.")

        logger.info("Month groups: %s", month_groups)
        timestamp_format = "yyyy-MM-dd HH:mm:ss"

        # Process each month group: read, transform, and write as Parquet locally
        for ym, files in month_groups.items():
            logger.info(f"Processing group for {ym} with {len(files)} file(s).")
            # Build absolute paths for files that exist.
            abs_csv_files = [os.path.abspath(csv_file) for csv_file in files if os.path.exists(os.path.abspath(csv_file))]
            if not abs_csv_files:
                logger.warning(f"No CSV files found for group {ym} that exist. Skipping.")
                continue

            logger.info(f"Reading {len(abs_csv_files)} CSV files for group {ym}.")
            # Read all CSVs at once; Spark will union them automatically.
            df = spark.read.csv(abs_csv_files, header=True, inferSchema=True)

            for col_name in df.columns:
                normalized_name = col_name.lower().replace(" ", "_")
                df = df.withColumnRenamed(col_name, normalized_name)


            for col_name in df.columns:
                if col_name in consistent_mapping:
                    new_name = consistent_mapping[col_name]
                    # Only rename if the names are different
                    if col_name != new_name:
                        df = df.withColumnRenamed(col_name, new_name)


            df = df.select([col(c).cast(t) for c, t in zip(df.columns, [f.dataType for f in schema.fields])])
            df = df.withColumn("birth_year", df["birth_year"].cast("int"))

            # Remove fractional seconds from starttime and stoptime columns
            df = df.withColumn("starttime", regexp_replace(col("starttime"), r"\.[0-9]+", ""))
            df = df.withColumn("stoptime", regexp_replace(col("stoptime"), r"\.[0-9]+", ""))

            df = df.withColumn(
                "starttime",
                when(
                    col("starttime").contains("/"),
                    to_timestamp(col("starttime"), "M/d/yyyy HH:mm:ss")
                ).otherwise(
                    to_timestamp(col("starttime"), "yyyy-MM-dd HH:mm:ss")
                )
            )

            df = df.withColumn(
                "stoptime",
                when(
                    col("stoptime").contains("/"),
                    to_timestamp(col("stoptime"), "M/d/yyyy HH:mm:ss")
                ).otherwise(
                    to_timestamp(col("stoptime"), "yyyy-MM-dd HH:mm:ss")
                )
            )
            df = df.withColumn("start_date", to_date(col("starttime")))
            df = df.withColumn("start_date", to_timestamp(col("starttime"), "yyyy-MM-dd HH:mm:ss"))

            #Debug
            '''
            df=df.limit(5)
            sample_str = df.toPandas().to_string()
            logger.info(f"Sample data for {ym}:\n{sample_str}")
            '''
            
            df = df.dropna()



            record_count = df.count()
            logger.info(f"Number of records in group {ym} (from {len(abs_csv_files)} files): {record_count}")
            if record_count == 0:
                logger.info(f"No records in group {ym}. Skipping.")
                continue

            # 1) Write to local file system under /spark_outputs/<year_month>
            local_output_dir = f"/spark_outputs/{ym}"
            logger.info(f"Writing data for {ym} locally to {local_output_dir}")
            try:
                df.write.mode("overwrite").parquet(f"file://{local_output_dir}")
                logger.info(f"Successfully written local Parquet for {ym}.")
            except Exception as write_ex:
                logger.error(f"Failed to write data for {ym} locally: {write_ex}", exc_info=True)
                continue

            # 2) Rename each part file to part-XXXX-year_month.parquet and upload to GCS
            #    Skip the _SUCCESS file.
            try:
                for fname in os.listdir(local_output_dir):
                    if fname == "_SUCCESS":
                        continue
                    if fname.startswith("part-") and fname.endswith(".parquet"):
                        # e.g., part-00000-xxxx.snappy.parquet -> part-00000-ym.parquet
                        part_num = fname.split("-")[1]  # "00000"
                        new_fname = f"part-{part_num}-{ym}.parquet"
                        old_path = os.path.join(local_output_dir, fname)
                        new_path = os.path.join(local_output_dir, new_fname)

                        os.rename(old_path, new_path)

                        logger.info(f"Uploading {new_fname} to gs://{GCS_BUCKET}/")
                        blob = bucket.blob(new_fname)
                        blob.upload_from_filename(new_path)
                        logger.info(f"Uploaded {new_fname} to GCS bucket {GCS_BUCKET}")
            except Exception as rename_upload_ex:
                logger.error(f"Error renaming/uploading part files for {ym}: {rename_upload_ex}", exc_info=True)

            # 3) Delete local directory to free memory
            try:
                shutil.rmtree(local_output_dir)
                logger.info(f"Removed local directory {local_output_dir} to free up space.")
            except Exception as cleanup_ex:
                logger.warning(f"Could not remove local directory {local_output_dir}: {cleanup_ex}")

        logger.info(f"All files for year {year} processed and uploaded to GCS.")
    except Exception as e:
        logger.error(f"Error in process_and_upload_year for {year}: {e}", exc_info=True)
    finally:
        spark.stop()
        gc.collect()


def cleanup_year_data(year):
    """Optionally remove extracted data for a year to free up disk space."""
    extract_path = os.path.join(EXTRACT_DIR, str(year))
    if os.path.exists(extract_path):
        shutil.rmtree(extract_path)
        logger.info(f"Cleaned up extracted data for year {year}.")

def create_external_table():
    """
    Create (or replace) an external BigQuery table that references the Parquet files in GCS.
    The external table will be partitioned by start_date (DATE) and clustered by bikeid.
    """
    # Define the GCS URI for Parquet files
    gcs_uri = f"gs://{GCS_BUCKET}/*.parquet"

    # Define the external table DDL query
    ddl = f"""
    CREATE OR REPLACE EXTERNAL TABLE `conicle-ai.Recommend.citibike_trips_external`
    OPTIONS (
      format = 'PARQUET',
      uris = ['{gcs_uri}']
    )
    """

    logger.info("Creating external table with DDL: %s", ddl)

    # Execute the query to create the external table
    query_job = client.query(ddl)
    query_job.result()  # Wait for the job to complete

    logger.info(f"External table {BQ_EXTERNAL_TABLE_FULL} created successfully.")


if __name__ == "__main__":
    # Create directories if they don't exist
    if not os.path.exists(ZIP_DIR):
        os.makedirs(ZIP_DIR)
    if not os.path.exists(EXTRACT_DIR):
        os.makedirs(EXTRACT_DIR)


    # Create GCS bucket for Parquet files if it doesn't exist
    create_gcs_bucket(GCS_BUCKET)

    # Process each year: download, extract, process, and then clean up.
    for year in YEARS:
        zip_file = download_zip(year)
        unzip_file(year, zip_file)
        process_and_upload_year(year)
        cleanup_year_data(year)

    create_external_table()

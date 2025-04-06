import urllib.request
import os
import logging
from pyspark.sql import SparkSession
from google.oauth2 import service_account

# --- Configure logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
file_handler = logging.FileHandler("/opt/spark/logs/zip_code_neighbor.log")
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

try:
    # --- GCP Auth ---
    service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/opt/gcp/google_credentials.json")
    credentials = service_account.Credentials.from_service_account_file(service_account_path)

    PROJECT_ID = "conicle-ai"
    DATASET_ID = "Recommend"
    TABLE_ID = "neighborhood"
    TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # --- Download CSV ---
    logger.info("Downloading NYC ZIP code CSV from GitHub...")
    csv_url = "https://raw.githubusercontent.com/erikgregorywebb/nyc-housing/refs/heads/master/Data/nyc-zip-codes.csv"
    local_path = "/tmp/nyc_zip_codes.csv"
    urllib.request.urlretrieve(csv_url, local_path)
    logger.info(f"CSV downloaded to {local_path}")

    # --- Start Spark session ---
    logger.info("Starting Spark session...")
    spark = SparkSession.builder \
        .appName("NYC Zip Codes to BigQuery") \
        .master("local[*]") \
        .config("spark.jars", "/opt/spark/jars/spark-3.5-bigquery-0.42.1.jar") \
        .config("viewsEnabled", "true") \
        .config("materializationDataset", "Recommend_temp") \
        .getOrCreate()

    # --- Load CSV into Spark ---
    logger.info("Reading CSV into Spark DataFrame...")
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(local_path)

    logger.info("CSV loaded. Schema:")
    df.printSchema()
    df.show(5)

    # --- Write to BigQuery ---
    logger.info(f"Writing DataFrame to BigQuery table: {TABLE_FULL_ID}")
    df.write.format("bigquery") \
        .option("table", TABLE_FULL_ID) \
        .option("credentialsFile", service_account_path) \
        .option("writeMethod", "direct") \
        .mode("overwrite") \
        .save()

    logger.info("Data successfully written to BigQuery!")

except Exception as e:
    logger.error(f"An error occurred: {e}", exc_info=True)

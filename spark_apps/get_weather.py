from pyspark.sql import SparkSession
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import logging


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
file_handler = logging.FileHandler("/opt/spark/logs/get_weather.log")
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# --- Config ---
PROJECT_ID = "conicle-ai"
DATASET_ID = "Recommend"
TABLE_ID = "weather_summary"
TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Credentials
service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/opt/gcp/google_credentials.json")
credentials = service_account.Credentials.from_service_account_file(service_account_path)

# --- BigQuery Check Function ---
def table_exists_and_has_data():
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    try:
        table = client.get_table(TABLE_FULL_ID)
        if table.num_rows > 0:
            logger.info(f"Table {TABLE_FULL_ID} already exists and has data.")
            return True
        else:
            logger.info(f"Table {TABLE_FULL_ID} exists but is empty.")
            return False
    except Exception as e:
        logger.info(f"Table {TABLE_FULL_ID} does not exist or cannot be accessed: {e}")
        return False

# --- Spark ETL ---
def run_weather_etl():
    logger.info("Starting Spark job to fetch NOAA weather data...")
    spark = SparkSession.builder \
        .appName("NOAA_Weather_ETL") \
        .master("local[*]") \
        .config("spark.jars", "/opt/spark/jars/spark-3.5-bigquery-0.42.1.jar") \
        .config("viewsEnabled", "true") \
        .config("materializationDataset", "Recommend_temp") \
        .getOrCreate()

    query = """
    SELECT
      TIMESTAMP(CONCAT(year, "-", mo, "-", da)) AS timestamp,
      AVG(IF(temp = 9999.9, NULL, temp)) AS temperature,
      AVG(IF(visib = 999.9, NULL, visib)) AS visibility,
      AVG(IF(wdsp = "999.9", NULL, CAST(wdsp AS FLOAT64))) AS wind_speed,
      AVG(IF(gust = 999.9, NULL, gust)) AS wind_gust,
      AVG(IF(prcp = 99.99, NULL, prcp)) AS precipitation,
      AVG(IF(sndp = 999.9, NULL, sndp)) AS snow_depth
    FROM
      `bigquery-public-data.noaa_gsod.gsod20*`
    WHERE
      CAST(year AS INT64) BETWEEN 2014 AND 2019
      AND (stn = "725030" OR stn = "744860")
    GROUP BY
      timestamp
    ORDER BY
      timestamp ASC
    """

    try:
        logger.info("Loading data from BigQuery...")
        df = spark.read.format("bigquery") \
            .option("project", PROJECT_ID) \
            .option("query", query) \
            .option("credentialsFile", service_account_path) \
            .load()

        record_count = df.count()
        logger.info(f"Fetched {record_count} records from BigQuery.")
    except Exception as read_ex:
        logger.error("Failed to load data from BigQuery", exc_info=True)
        return  # Stop execution if read fails

    if record_count == 0:
        logger.warning("No records fetched. Skipping write operation.")
        return

    try:
        logger.info("Writing data to BigQuery...")
        df.write.format("bigquery") \
            .option("table", TABLE_FULL_ID) \
            .option("credentialsFile", service_account_path) \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()
        logger.info("Data successfully written to BigQuery.")
    except Exception as write_ex:
        logger.error("Failed to write data to BigQuery", exc_info=True)

    except Exception as e:
        logger.error("Unexpected error in NOAA Weather ETL job", exc_info=True)
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")

# --- Main Entry ---
if __name__ == "__main__":
    if not table_exists_and_has_data():
        run_weather_etl()
    else:
        logger.info("ETL skipped — table already populated.")

import os
import requests
import zipfile
import geopandas as gpd
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import col,expr
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
import math
import gc

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Constants
ZIP_URL = "https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_zcta510_500k.zip"
ZIP_FILE = "cb_2018_us_zcta510_500k.zip"
EXTRACT_FOLDER = "zcta_shapefile"
SHAPEFILE_NAME = "cb_2018_us_zcta510_500k.shp"

# BigQuery Configuration
BQ_PROJECT = "conicle-ai"
BQ_DATASET = "Recommend"
BQ_TABLE = "zip_code_boundaries"
BQ_TABLE_FULL = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"


# Load Google Cloud credentials
service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/opt/gcp/google_credentials.json")
credentials = service_account.Credentials.from_service_account_file(service_account_path)

# Initialize BigQuery client
client = bigquery.Client(credentials=credentials, project=BQ_PROJECT)


def download_zip():
    """Download ZIP file from the Census Bureau if it doesn't exist."""
    if not os.path.exists(ZIP_FILE):
        logger.info(f"Downloading {ZIP_FILE} ...")
        response = requests.get(ZIP_URL, stream=True)
        with open(ZIP_FILE, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        logger.info("Download complete.")
    else:
        logger.info("ZIP file already exists, skipping download.")


def unzip_file():
    """Unzip the shapefile if not already extracted."""
    if not os.path.exists(EXTRACT_FOLDER):
        logger.info(f"Extracting {ZIP_FILE} ...")
        with zipfile.ZipFile(ZIP_FILE, "r") as zip_ref:
            zip_ref.extractall(EXTRACT_FOLDER)
        logger.info("Extraction complete.")
    else:
        logger.info("Shapefile already extracted, skipping extraction.")


def create_bigquery_table():
    """Check if BigQuery table exists; if not, create it."""
    try:
        client.get_table(BQ_TABLE_FULL)
        logger.info("BigQuery table exists.")
    except NotFound:
        logger.warning("Table not found, creating table...")

        table_schema = [
            bigquery.SchemaField("zip_code", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("state_code", "STRING"),
            bigquery.SchemaField("state_name", "STRING"),
            bigquery.SchemaField("area_land_meters", "FLOAT"),
            bigquery.SchemaField("area_water_meters", "FLOAT"),
            bigquery.SchemaField("latitude", "FLOAT"),
            bigquery.SchemaField("longitude", "FLOAT"),
        ]


        table = bigquery.Table(BQ_TABLE_FULL, schema=table_schema)
        client.create_table(table)
        logger.info("BigQuery table created successfully.")


def process_and_upload(chunk_size=2000):
    try:
        spark = SparkSession.builder \
            .appName("ZCTA_Shapes_To_BigQuery") \
            .config("spark.jars", "/opt/spark/jars/spark-bigquery-with-dependencies_2.13-0.42.1.jar") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .getOrCreate()


        shapefile_path = os.path.join(EXTRACT_FOLDER, SHAPEFILE_NAME)
        gdf = gpd.read_file(shapefile_path)
        logger.info(f"Total rows: {len(gdf)}")

        # Column mapping
        column_mapping = {
            "ZCTA5CE10": "zip_code",
            "AFFGEOID10": "state_code",
            "GEOID10": "state_name",
            "ALAND10": "area_land_meters",
            "AWATER10": "area_water_meters",
        }
        gdf = gdf.rename(columns={k: v for k, v in column_mapping.items() if k in gdf.columns})

        if "geometry" in gdf.columns:
            gdf["centroid"] = gdf["geometry"].centroid
            gdf["latitude"] = gdf["centroid"].y
            gdf["longitude"] = gdf["centroid"].x
            logger.info("Geometry column converted to centroid lat/lng.")
        else:
            gdf["latitude"] = None
            gdf["longitude"] = None
            logger.warning("No geometry column found; lat/lng will be null.")



        required_columns = [
            "zip_code", "state_code", "state_name",
            "area_land_meters", "area_water_meters", "latitude", "longitude"
        ]

        gdf = gdf[[col for col in required_columns if col in gdf.columns]]
        gdf["area_land_meters"] = gdf["area_land_meters"].astype(float)
        gdf["area_water_meters"] = gdf["area_water_meters"].astype(float)
        logger.info(f"Initial GeoDataFrame shape: {gdf.shape} (rows, columns)")
        # Chunking logic
        num_chunks = math.ceil(len(gdf) / chunk_size)
        logger.info(f"Processing in {num_chunks} chunks of {chunk_size} rows.")

        for i in range(num_chunks):
            chunk = gdf.iloc[i * chunk_size:(i + 1) * chunk_size].copy()
            logger.info(f"Uploading chunk {i+1}/{num_chunks} with {len(chunk)} rows.")

            schema = StructType([
                StructField("zip_code", StringType(), False),
                StructField("state_code", StringType(), True),
                StructField("state_name", StringType(), True),
                StructField("area_land_meters", FloatType(), True),
                StructField("area_water_meters", FloatType(), True),
                StructField("latitude", FloatType(), True),
                StructField("longitude", FloatType(), True),
            ])


            spark_df = spark.createDataFrame(chunk, schema=schema)
            spark_df = spark_df.withColumn("zip_code", col("zip_code").cast("string"))

            spark_df.write \
                .format("bigquery") \
                .option("table", BQ_TABLE_FULL) \
                .option("credentialsFile", service_account_path) \
                .option("writeMethod", "direct") \
                .mode("append") \
                .save()

            # Explicit cleanup
            del chunk, spark_df
            gc.collect()
            logger.info(f"Chunk {i+1}/{num_chunks} uploaded and memory freed.")

        logger.info("All data successfully uploaded to BigQuery in chunks.")

    except Exception as e:
        logger.error(f"Error in process_and_upload_in_chunks: {e}", exc_info=True)

    finally:
        spark.stop()




if __name__ == "__main__":
    download_zip()
    unzip_file()
    create_bigquery_table()
    process_and_upload()

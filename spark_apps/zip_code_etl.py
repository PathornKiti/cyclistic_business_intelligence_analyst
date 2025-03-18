import os
import requests
import zipfile
import geopandas as gpd
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import col
from google.cloud import bigquery
from google.oauth2 import service_account

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
BQ_DATASET = "geo_data"
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
    except Exception:
        logger.warning("Table not found, creating table...")

        table_schema = [
            bigquery.SchemaField("zip_code", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("state_code", "STRING"),
            bigquery.SchemaField("state_name", "STRING"),
            bigquery.SchemaField("county_name", "STRING"),
            bigquery.SchemaField("area_land_meters", "FLOAT"),
            bigquery.SchemaField("area_water_meters", "FLOAT"),
            bigquery.SchemaField("internal_point_lat", "FLOAT"),
            bigquery.SchemaField("internal_point_lon", "FLOAT"),
            bigquery.SchemaField("zip_code_geom", "GEOGRAPHY")
        ]

        table = bigquery.Table(BQ_TABLE_FULL, schema=table_schema)
        client.create_table(table)
        logger.info("BigQuery table created successfully.")


def process_and_upload():
    """Load the shapefile, process data, and upload it to BigQuery via Spark."""
    spark = SparkSession.builder \
        .appName("ZCTA_Shapes_To_BigQuery") \
        .config("spark.jars", "/opt/spark/jars/spark-bigquery-with-dependencies_2.13-0.42.1.jar") \
        .getOrCreate()

    # Load the shapefile using GeoPandas
    shapefile_path = os.path.join(EXTRACT_FOLDER, SHAPEFILE_NAME)
    gdf = gpd.read_file(shapefile_path)

    # Rename relevant columns
    gdf = gdf.rename(columns={
        "ZCTA5CE10": "zip_code",
        "GEOID10": "state_code",
        "NAMELSAD10": "state_name",
        "LSAD10": "county_name",
        "ALAND": "area_land_meters",
        "AWATER": "area_water_meters",
        "INTPTLAT10": "internal_point_lat",
        "INTPTLON10": "internal_point_lon",
    })

    # Convert geometry to WKT format for BigQuery GEOGRAPHY type
    gdf["zip_code_geom"] = gdf["geometry"].apply(lambda x: x.wkt)

    # Convert GeoPandas DataFrame to Pandas DataFrame
    df = gdf[["zip_code", "state_code", "state_name", "county_name", "area_land_meters",
              "area_water_meters", "internal_point_lat", "internal_point_lon", "zip_code_geom"]]

    # Define schema for Spark
    schema = StructType([
        StructField("zip_code", StringType(), False),
        StructField("state_code", StringType(), True),
        StructField("state_name", StringType(), True),
        StructField("county_name", StringType(), True),
        StructField("area_land_meters", FloatType(), True),
        StructField("area_water_meters", FloatType(), True),
        StructField("internal_point_lat", FloatType(), True),
        StructField("internal_point_lon", FloatType(), True),
        StructField("zip_code_geom", StringType(), True)  # Will be stored as GEOGRAPHY in BigQuery
    ])

    # Convert Pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(df, schema=schema)

    # Ensure zip_code is a string
    spark_df = spark_df.withColumn("zip_code", col("zip_code").cast("string"))

    # Write to BigQuery
    spark_df.write \
        .format("bigquery") \
        .option("table", BQ_TABLE_FULL) \
        .option("credentialsFile", service_account_path) \
        .option("writeMethod", "direct")\
        .mode("append") \
        .save()

    logger.info("Data successfully uploaded to BigQuery.")

    # Stop Spark session
    spark.stop()


if __name__ == "__main__":
    download_zip()
    unzip_file()
    create_bigquery_table()
    process_and_upload()

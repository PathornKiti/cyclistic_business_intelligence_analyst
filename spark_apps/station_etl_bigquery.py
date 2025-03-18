from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, TimestampType
from pyspark.sql.functions import col, concat_ws,from_unixtime
import requests
import json
import os
import logging
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.bigquery import SchemaField

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CitibikeToBigQuery") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-3.5-bigquery:0.42.1") \
    .getOrCreate()

# Define BigQuery table
BQ_PROJECT = "conicle-ai"
BQ_DATASET = "Recommend"
BQ_TABLE = "citibike_stations"
BQ_TABLE_FULL = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

# Load service account credentials
service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/opt/gcp/google_credentials.json")
credentials = service_account.Credentials.from_service_account_file(service_account_path)

# Initialize BigQuery client with credentials
client = bigquery.Client(credentials=credentials, project=BQ_PROJECT)

# Define schema for BigQuery
schema = StructType([
    StructField("station_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("short_name", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("region_id", IntegerType(), True),
    StructField("rental_methods", StringType(), True),
    StructField("capacity", IntegerType(), True),
    StructField("eightd_has_key_dispenser", BooleanType(), True),
    StructField("num_bikes_available", IntegerType(), True),
    StructField("num_bikes_disabled", IntegerType(), True),
    StructField("num_docks_available", IntegerType(), True),
    StructField("num_docks_disabled", IntegerType(), True),
    StructField("is_installed", BooleanType(), True),
    StructField("is_renting", BooleanType(), True),
    StructField("is_returning", BooleanType(), True),
    StructField("eightd_has_available_keys", BooleanType(), True),
    StructField("last_reported", TimestampType(), True)
])

try:
    # Fetch station status data
    STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
    status_response = requests.get(STATUS_URL)
    status_response.raise_for_status()
    status_data = status_response.json()["data"]["stations"]
    status_df = spark.createDataFrame(status_data)
    logger.info("Fetched station status data successfully.")

    # Fetch station information data
    INFO_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
    info_response = requests.get(INFO_URL)
    info_response.raise_for_status()
    info_data = info_response.json()["data"]["stations"]
    info_df = spark.createDataFrame(info_data)
    logger.info("Fetched station information data successfully.")

    # Select relevant columns from info data
    info_df = info_df.select(
        "station_id", "name", "short_name", "lat", "lon", "region_id", "rental_methods", "capacity", "eightd_has_key_dispenser"
    )

    # Rename lat/lon columns to match schema
    info_df = info_df.withColumnRenamed("lat", "latitude").withColumnRenamed("lon", "longitude")

    # Join station information with station status on station_id
    merged_df = info_df.join(status_df, "station_id", "inner")

    expected_columns = [
    "station_id", "name", "short_name", "latitude", "longitude", "region_id",
    "rental_methods", "capacity", "eightd_has_key_dispenser", "num_bikes_available",
    "num_bikes_disabled", "num_docks_available", "num_docks_disabled",
    "is_installed", "is_renting", "is_returning", "eightd_has_available_keys", "last_reported"
    ]

    # Drop excess columns
    merged_df = merged_df.select(*expected_columns)

    merged_df = merged_df.withColumn("is_installed", col("is_installed").cast("boolean"))
    merged_df = merged_df.withColumn("is_renting", col("is_renting").cast("boolean"))
    merged_df = merged_df.withColumn("is_returning", col("is_returning").cast("boolean"))
    merged_df = merged_df.withColumn("eightd_has_key_dispenser", col("eightd_has_key_dispenser").cast("boolean"))
    merged_df = merged_df.withColumn("eightd_has_available_keys", col("eightd_has_available_keys").cast("boolean"))
    merged_df = merged_df.withColumn("region_id", col("region_id").cast("int"))
    merged_df = merged_df.withColumn("rental_methods", concat_ws(",", col("rental_methods")))
    merged_df = merged_df.withColumn("last_reported", from_unixtime(col("last_reported")).cast("timestamp"))


    def convert_schema(pyspark_schema):
        type_mapping = {
            "string": "STRING",
            "int": "INTEGER",
            "bigint": "INTEGER",
            "double": "FLOAT",
            "float": "FLOAT",
            "boolean": "BOOLEAN",
            "timestamp": "TIMESTAMP"
        }
        return [
            SchemaField(field.name, type_mapping.get(field.dataType.simpleString().lower(), "STRING"))
            for field in pyspark_schema
        ]


    # Check if table exists in BigQuery, if not create it
    try:
        client.get_table(BQ_TABLE_FULL)  # Check if table exists
        logger.info("BigQuery table exists.")
    except Exception as e:
        logger.warning("Table not found, creating table...")
        table_schema = convert_schema(schema)
        table = bigquery.Table(BQ_TABLE_FULL, schema=table_schema)
        client.create_table(table)
        logger.info("BigQuery table created successfully.")

    # Write to BigQuery with schema handling
    merged_df.write \
        .format("bigquery") \
        .option("table", BQ_TABLE_FULL) \
        .option("credentialsFile", service_account_path) \
        .mode("append") \
        .option("writeMethod", "direct")\
        .save()

    logger.info("Data successfully uploaded to BigQuery.")

except Exception as e:
    logger.error(f"Error occurred: {e}")

finally:
    # Stop Spark session
    spark.stop()
    logger.info("Spark session stopped.")

import os
import logging
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pyspark.sql import SparkSession
from google.cloud import bigquery
from google.oauth2 import service_account

# ───────────────────────────────
# 📘 Logging Setup
# ───────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
file_handler = logging.FileHandler("/opt/spark/logs/station_location.log")
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# ───────────────────────────────
# 🚀 Spark Session Init
# ───────────────────────────────
spark = SparkSession.builder.appName("CitiBikeFlow").getOrCreate()

# ───────────────────────────────
# 🧠 GCP / BigQuery Setup
# ───────────────────────────────
# 🧠 GCP / BigQuery Setup
# ───────────────────────────────
BQ_PROJECT = "your_project_id"
BQ_DATASET = "your_dataset"
BQ_TABLE = "your_table_name"
BQ_TABLE_FULL = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/opt/gcp/google_credentials.json")
credentials = service_account.Credentials.from_service_account_file(service_account_path)
client = bigquery.Client(credentials=credentials, project=BQ_PROJECT)

# ───────────────────────────────
# 📥 Step 1: Download Station Data
# ───────────────────────────────
def request_station_data():
    logger.info("📥 Fetching station data...")
    status_url = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
    info_url = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
    
    status_data = requests.get(status_url).json()["data"]["stations"]
    info_data = requests.get(info_url).json()["data"]["stations"]
    
    status_df = pd.DataFrame(status_data)
    info_df = pd.DataFrame(info_data)
    merged_df = pd.merge(info_df, status_df, on="station_id", how="inner")

    keep_columns = ["station_id", "name", "short_name", "lat", "lon", "region_id", "capacity"]
    cleaned_df = merged_df[keep_columns].copy()
    cleaned_df = cleaned_df.rename(columns={"lat": "latitude", "lon": "longitude"})
    
    logger.info("✅ Station data cleaned.")
    return cleaned_df

# ───────────────────────────────
# 🌐 Step 2: Load NTA Geometry (GeoJSON)
# ───────────────────────────────
def load_nta_shapes() -> gpd.GeoDataFrame:
    logger.info("🌐 Downloading NTA boundaries from NYC Open Data...")

    url = "https://data.cityofnewyork.us/resource/9nt8-h7nd.geojson"
    gdf = gpd.read_file(url)
    gdf = gdf.to_crs("EPSG:4326")  # Ensure proper CRS

    logger.info(f"🔍 Columns found: {list(gdf.columns)}")

    # Fix: lowercase keys
    gdf = gdf.rename(columns={
        "boroname": "borough",
        "ntaname": "neighborhood"
    })

    logger.info("✅ NTA shapefile with borough/neighborhood loaded.")
    return gdf[["borough", "neighborhood", "geometry"]]



# ───────────────────────────────
# 📌 Step 3: Spatial Join to Enrich Station Data
# ───────────────────────────────
def enrich_with_nta(station_df: pd.DataFrame, nta_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    logger.info("📌 Enriching station data with NTA neighborhoods...")

    station_gdf = gpd.GeoDataFrame(
        station_df,
        geometry=gpd.points_from_xy(station_df["longitude"], station_df["latitude"]),
        crs="EPSG:4326"
    )

    joined = gpd.sjoin(station_gdf, nta_gdf, how="left", predicate="within")

    enriched = joined.drop(columns=["geometry", "index_right"])
    logger.info("✅ Spatial enrichment complete.")
    return pd.DataFrame(enriched)

# ───────────────────────────────
# 🛢️ Step 4: Upload to BigQuery
# ───────────────────────────────
def upload_to_bigquery(df: pd.DataFrame):
    logger.info("🚀 Preparing data for BigQuery upload...")

    df["region_id"] = pd.to_numeric(df["region_id"], errors="coerce").astype("Int64")
    df["capacity"] = pd.to_numeric(df["capacity"], errors="coerce").astype("Int64")

    df["borough"] = df["borough"].astype(str)
    df["neighborhood"] = df["neighborhood"].astype(str)

    df = df.dropna(subset=["borough", "neighborhood"])

    df = df[~df["borough"].str.lower().eq("nan")]
    df = df[~df["neighborhood"].str.lower().eq("nan")]

    required_columns = [
        "station_id", "name", "short_name", "latitude", "longitude", 
        "region_id", "capacity", "borough", "neighborhood"
    ]
    df = df.dropna(subset=required_columns)

    schema = [
        bigquery.SchemaField("station_id", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("short_name", "STRING"),
        bigquery.SchemaField("latitude", "FLOAT"),
        bigquery.SchemaField("longitude", "FLOAT"),
        bigquery.SchemaField("region_id", "INTEGER"),
        bigquery.SchemaField("capacity", "INTEGER"),
        bigquery.SchemaField("borough", "STRING"),
        bigquery.SchemaField("neighborhood", "STRING")
    ]

    client.delete_table(BQ_TABLE_FULL, not_found_ok=True)
    table = bigquery.Table(BQ_TABLE_FULL, schema=schema)
    client.create_table(table)

    logger.info("📤 Uploading to BigQuery...")
    job = client.load_table_from_dataframe(df, BQ_TABLE_FULL)
    job.result()
    logger.info("✅ Upload to BigQuery successful.")

# ───────────────────────────────
# 🧩 Step 5: Run Pipeline
# ───────────────────────────────
def run_pipeline():
    logger.info("🚦 Starting ETL pipeline for Citi Bike station locations with neighborhood info...")
    station_df = request_station_data()
    nta_gdf = load_nta_shapes()
    enriched_df = enrich_with_nta(station_df, nta_gdf)
    upload_to_bigquery(enriched_df)
    logger.info("🎉 Pipeline completed successfully.")

# ───────────────────────────────
# 🔁 Trigger Execution
# ───────────────────────────────
if __name__ == "__main__":
    run_pipeline()

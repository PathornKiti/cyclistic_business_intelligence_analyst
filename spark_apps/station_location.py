import os
import logging
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely import wkt
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
BQ_PROJECT = "conicle-ai"
BQ_DATASET = "Recommend"
BQ_TABLE = "citibike_stations_location"
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
# 🌐 Step 2A: Load ZIP Code Shapes (Polygon Geometry)
# ───────────────────────────────
def load_zipcode_shapes() -> gpd.GeoDataFrame:
    url = "https://data.cityofnewyork.us/api/views/pri4-ifjk/rows.csv?date=20250410&accessType=DOWNLOAD"
    logger.info("🌐 Downloading ZIP Code geometry from NYC Open Data...")
    
    df = pd.read_csv(url)
    geometry = gpd.GeoSeries.from_wkt(df["the_geom"])
    gdf = gpd.GeoDataFrame(df[["MODZCTA"]].copy(), geometry=geometry, crs="EPSG:4326")
    
    logger.info("✅ ZIP Code shapefile loaded and parsed.")
    return gdf

# ───────────────────────────────
# 🌐 Step 2B: Load ZIP → Borough/Neighborhood Metadata
# ───────────────────────────────
def load_zipcode_metadata() -> pd.DataFrame:
    meta_url = "https://raw.githubusercontent.com/erikgregorywebb/nyc-housing/refs/heads/master/Data/nyc-zip-codes.csv"
    logger.info("🌐 Downloading ZIP code metadata (borough, neighborhood)...")

    meta_df = pd.read_csv(meta_url)
    meta_df = meta_df.rename(columns={
        "ZipCode": "zipcode",
        "Borough": "borough",
        "Neighborhood": "neighborhood"
    })

    # Clean and convert ZIPs to integer
    meta_df["zipcode"] = pd.to_numeric(meta_df["zipcode"], errors="coerce").astype("Int64")
    meta_df["borough"] = meta_df["borough"].astype(str)
    meta_df["neighborhood"] = meta_df["neighborhood"].astype(str)

    logger.info("✅ Metadata loaded and standardized.")
    return meta_df[["zipcode", "borough", "neighborhood"]]


# ───────────────────────────────
# 📌 Step 3: Spatial Join + Enrichment
# ───────────────────────────────
def enrich_with_zipcodes(station_df: pd.DataFrame, zipcode_gdf: gpd.GeoDataFrame, zipcode_meta: pd.DataFrame) -> pd.DataFrame:
    logger.info("📌 Enriching station data with ZIP codes...")

    station_gdf = gpd.GeoDataFrame(
        station_df,
        geometry=gpd.points_from_xy(station_df["longitude"], station_df["latitude"]),
        crs="EPSG:4326"
    )

    joined = gpd.sjoin(station_gdf, zipcode_gdf, how="left", predicate="within")
    enriched = pd.DataFrame(joined.drop(columns=["geometry", "index_right"]))
    enriched = enriched.rename(columns={"MODZCTA": "zipcode"})
    enriched["zipcode"] = pd.to_numeric(enriched["zipcode"], errors="coerce").astype("Int64")
    enriched = enriched.dropna(subset=["zipcode"])

    logger.info("🔗 Joining with borough and neighborhood metadata...")
    enriched = pd.merge(enriched, zipcode_meta, on="zipcode", how="left")

    logger.info("✅ ZIP code, borough, and neighborhood enrichment complete.")
    return enriched

# ───────────────────────────────
# 🛢️ Step 4: Upload to BigQuery
# ───────────────────────────────
def upload_to_bigquery(df: pd.DataFrame):
    logger.info("🚀 Preparing data for BigQuery upload...")

    df["region_id"] = pd.to_numeric(df["region_id"], errors="coerce").astype("Int64")
    df["capacity"] = pd.to_numeric(df["capacity"], errors="coerce").astype("Int64")

    df["borough"] = df["borough"].astype(str)
    df["neighborhood"] = df["neighborhood"].astype(str)

    # Drop true NaNs before converting to string
    df = df.dropna(subset=["borough", "neighborhood"])

    # Also filter out any lingering 'nan' strings
    df = df[~df["borough"].str.lower().eq("nan")]
    df = df[~df["neighborhood"].str.lower().eq("nan")]
    # Drop rows with missing required data
    required_columns = [
        "station_id", "name", "latitude", "longitude", 
        "region_id", "capacity", "zipcode", 
        "borough", "neighborhood"
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
        bigquery.SchemaField("zipcode", "INTEGER"),
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
    logger.info("🚦 Starting ETL pipeline for Citi Bike ZIP codes + borough/neighborhood...")
    station_df = request_station_data()
    zipcode_gdf = load_zipcode_shapes()
    zipcode_meta = load_zipcode_metadata()
    enriched_df = enrich_with_zipcodes(station_df, zipcode_gdf, zipcode_meta)
    upload_to_bigquery(enriched_df)
    logger.info("🎉 Pipeline completed successfully.")

# ───────────────────────────────
# 🔁 Trigger Execution
# ───────────────────────────────
if __name__ == "__main__":
    run_pipeline()

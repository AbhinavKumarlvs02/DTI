import polars as pl
import requests
import os
import glob # Add this import at the top

# --- CONFIGURATION ---
YEAR = "2023"
MONTHS = ["01", "02", "03", "04", "05", "06"] # 6 months is plenty for pre-training
DOWNLOAD_DIR = "./raw_nyc_data"
PROCESSED_FILE = "./clean_nyc_training_data.parquet"

# Create directory for raw data if it doesn't exist
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def download_nyc_parquet(year, month):
    """Downloads the official NYC TLC Parquet file for a specific month."""
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"
    file_path = f"{DOWNLOAD_DIR}/yellow_tripdata_{year}-{month}.parquet"
    
    if not os.path.exists(file_path):
        print(f"Downloading NYC data for {year}-{month}...")
        response = requests.get(url, stream=True)
        with open(file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024 * 1024): # 1MB chunks
                if chunk:
                    file.write(chunk)
        print(f"Saved: {file_path}")
    else:
        print(f"File {file_path} already exists. Skipping download.")

def process_and_clean_data():
    """Uses Polars LazyFrames to clean massive data without crashing RAM."""
    print("\nStarting out-of-core data cleaning using Polars...")
    
    lazy_dfs = []
    file_pattern = f"{DOWNLOAD_DIR}/*.parquet"
    
    # We only care about these 5 columns. Ignore the rest of the messy schema.
    required_columns = [
        "tpep_pickup_datetime", 
        "tpep_dropoff_datetime", # Needed temporarily to filter trip duration
        "PULocationID", 
        "trip_distance", 
        "fare_amount"
    ]
    
    for file_path in glob.glob(file_pattern):
        # 1. Scan individual file
        ldf = pl.scan_parquet(file_path)
        
        # 2. Immediately drop all the mismatched columns (like Airport_fee)
        ldf = ldf.select(required_columns)
        
        # 3. Force strict uniform types on our selected columns
        ldf = ldf.with_columns([
            pl.col("PULocationID").cast(pl.Int64),
            pl.col("trip_distance").cast(pl.Float64),
            pl.col("fare_amount").cast(pl.Float64)
        ])
        
        lazy_dfs.append(ldf)
        
    # 4. NOW concatenate them. They all have exactly 5 columns with identical data types.
    lazy_df = pl.concat(lazy_dfs)
    
    # 5. FILTER: Strip out the garbage data and anomalies mathematically
    clean_lazy_df = (
        lazy_df
        .filter(
            (pl.col("trip_distance") > 0.1) & (pl.col("trip_distance") < 100.0) &
            (pl.col("fare_amount") > 2.50) & (pl.col("fare_amount") < 300.0) &
            ((pl.col("tpep_dropoff_datetime") - pl.col("tpep_pickup_datetime")).dt.total_minutes() > 1.0) &
            (pl.col("PULocationID").is_not_null())
        )
        # 6. Final Select: Drop dropoff_datetime (we only needed it for the filter above)
        .select([
            "tpep_pickup_datetime", 
            "PULocationID", 
            "trip_distance", 
            "fare_amount"
        ])
    )

    print("Executing filters and writing final cleaned dataset...")
    # 7. Execute the streaming pipeline
    clean_lazy_df.sink_parquet(PROCESSED_FILE)
    
    print(f"\nSuccess! Clean data saved to {PROCESSED_FILE}")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    # Step 1: Download the raw files
    for month in MONTHS:
        download_nyc_parquet(YEAR, month)
        
    # Step 2: Clean and compress into a single training file
    process_and_clean_data()
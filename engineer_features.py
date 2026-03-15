import polars as pl
import math

# --- CONFIGURATION ---
INPUT_FILE = "./clean_nyc_training_data.parquet"
OUTPUT_FILE = "./stgcn_nyc_features.parquet"

# Standard NYC Base Fare Approximation (Base rate + per mile rate)
# We use this to calculate how much the ride *should* have cost without surge/traffic
BASE_FEE = 50.00
PER_MILE_RATE = 15.00

def engineer_features():
    print("Loading clean dataset...")
    # We can use read_parquet here if your machine has at least 8GB of RAM, 
    # as the clean file is much smaller. Otherwise, use scan_parquet() and collect() at the end.
    df = pl.read_parquet(INPUT_FILE)

    print("Engineering Target Labels and Cyclical Time Features...")
    
    engineered_df = (
        df
        .with_columns([
            # 1. CALCULATE BASE FARE
            (pl.lit(BASE_FEE) + (pl.col("trip_distance") * PER_MILE_RATE)).alias("calculated_base_fare"),
            
            # 2. EXTRACT TIME COMPONENTS
            pl.col("tpep_pickup_datetime").dt.hour().alias("hour"),
            pl.col("tpep_pickup_datetime").dt.weekday().alias("day_of_week") # 1 = Monday, 7 = Sunday
        ])
        .with_columns([
            # 3. CALCULATE TARGET LABEL (True Surge Multiplier)
            # If actual fare is $15 and base fare is $10, surge is 1.5x
            (pl.col("fare_amount") / pl.col("calculated_base_fare")).alias("true_surge_multiplier"),
            
            # 4. ENCODE CYCLICAL TIME (Sine and Cosine for 24 hours)
            # This ensures the AI knows 23:00 and 01:00 are mathematically close
            (pl.col("hour") * (2.0 * math.pi / 24.0)).sin().alias("time_sin"),
            (pl.col("hour") * (2.0 * math.pi / 24.0)).cos().alias("time_cos")
        ])
        .filter(
            # Sanity check: Surge should rarely be less than 1.0 (discounts) 
            # and rarely above 5.0 (extreme outliers)
            (pl.col("true_surge_multiplier") >= 1.0) & 
            (pl.col("true_surge_multiplier") <= 5.0)
        )
        .select([
            # Final Feature Vector for the Neural Network
            "tpep_pickup_datetime",
            "PULocationID",  # The Spatial Node
            "time_sin",      # Temporal Feature 1
            "time_cos",      # Temporal Feature 2
            "day_of_week",   # Temporal Feature 3
            "true_surge_multiplier"  # The Target Label (Y)
        ])
    )

    print("Saving engineered feature vectors...")
    engineered_df.write_parquet(OUTPUT_FILE)
    print(f"Success! Ready for PyTorch. Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    engineer_features()
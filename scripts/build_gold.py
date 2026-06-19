import pandas as pd
import os
import logging
from datetime import datetime
from io import BytesIO
from minio import Minio

# =====================================
# LOGGING
# =====================================
logging.basicConfig(
    filename="pipeline_gold.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# =====================================
# MINIO CLIENT
# =====================================
def get_minio_client():
    return Minio(
          "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

# =====================================
# 1. DIM CRYPTO (CLEAN + STABLE KEYS)
# =====================================
def build_dim_crypto(df):

    df = df.dropna(subset=["id", "symbol", "name"]).copy()

    # normalize keys
    df["id"] = df["id"].str.lower().str.strip()
    df["symbol"] = df["symbol"].str.lower().str.strip()
    df["name"] = df["name"].str.strip()

    dim_crypto = df[["id", "symbol", "name"]].drop_duplicates().reset_index(drop=True)

    dim_crypto["crypto_key"] = dim_crypto.index + 1

    return dim_crypto


# =====================================
# 2. DIM DATE (FIX GRANULARITY + NO NULLS)
# =====================================
def build_dim_date(df):

    df = df.copy()

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    dim_date = df[["timestamp"]].drop_duplicates().reset_index(drop=True)

    dim_date["date"] = dim_date["timestamp"].dt.date
    dim_date["year"] = dim_date["timestamp"].dt.year
    dim_date["month"] = dim_date["timestamp"].dt.month
    dim_date["day"] = dim_date["timestamp"].dt.day
    dim_date["hour"] = dim_date["timestamp"].dt.hour
    dim_date["minute"] = dim_date["timestamp"].dt.minute
    dim_date["week"] = dim_date["timestamp"].dt.isocalendar().week.astype(int)
    dim_date["quarter"] = dim_date["timestamp"].dt.quarter

    dim_date["date_key"] = dim_date.index + 1

    return dim_date


# =====================================
# 3. FACT TABLE (CLEAN JOIN SAFE)
# =====================================
def build_fact_table(df, dim_crypto, dim_date):

    df = df.copy()

    # normalize keys
    df["id"] = df["id"].str.lower().str.strip()
    df["symbol"] = df["symbol"].str.lower().str.strip()

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # join crypto
    fact = df.merge(
        dim_crypto,
        on=["id", "symbol", "name"],
        how="left"
    )

    # join date (IMPORTANT FIX)
    fact = fact.merge(
        dim_date,
        on=["timestamp"],
        how="left"
    )

    # DEBUG NULLS
    print("NULL crypto_key:", fact["crypto_key"].isna().sum())
    print("NULL date_key:", fact["date_key"].isna().sum())

    # REMOVE INVALID ROWS
    fact = fact.dropna(subset=["crypto_key", "date_key"])

    fact = fact[[
        "crypto_key",
        "date_key",
        "current_price",
        "market_cap",
        "market_cap_rank",
        "total_volume",
        "high_24h",
        "low_24h",
        "price_change_24h",
        "price_change_percentage_24h",
        "circulating_supply",
        "max_supply",
        "ath"
    ]].reset_index(drop=True)

    return fact


# =====================================
# 4. SAVE GOLD (LOCAL + MINIO)
# =====================================
def save_gold(client, dim_crypto, dim_date, fact):

    bucket = "crypto-gold"

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    today = datetime.now()
    base_path = f"{today.year}/{today.month:02d}/{today.day:02d}"

    tables = {
        "dim_crypto": dim_crypto,
        "dim_date": dim_date,
        "fact_crypto_snapshot": fact
    }

    for name, df in tables.items():

        df = df.reset_index(drop=True)

        # LOCAL SAVE
        local_dir = f"../data/gold/{base_path}"
        os.makedirs(local_dir, exist_ok=True)

        local_file = f"{local_dir}/{name}.parquet"
        df.to_parquet(local_file, index=False)

        # MINIO SAVE
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        client.put_object(
            bucket,
            f"{base_path}/{name}.parquet",
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type="application/octet-stream"
        )

        logger.info(f"Saved {name} shape={df.shape}")


# =====================================
# 5. MAIN PIPELINE
# =====================================
def gold_pipeline(df):

    print("🚀 START GOLD PIPELINE")

    # CLEAN INPUT (IMPORTANT FIX)
    df = df.dropna(subset=["timestamp", "id", "symbol", "name"]).copy()
    df = df.drop_duplicates()

    print("📊 INPUT SHAPE:", df.shape)

    dim_crypto = build_dim_crypto(df)
    dim_date = build_dim_date(df)
    fact = build_fact_table(df, dim_crypto, dim_date)

    print("📊 DIM_CRYPTO:", dim_crypto.shape)
    print("📊 DIM_DATE:", dim_date.shape)
    print("📊 FACT:", fact.shape)

    client = get_minio_client()
    save_gold(client, dim_crypto, dim_date, fact)
    #############
    print("\nDATES GOLD")

    print(
        sorted(
            dim_date["date"].unique()
        )
    )
    print("✅ GOLD DONE")
    return dim_crypto, dim_date, fact
   
    ####################
    
# =====================================
# RUN
# =====================================
if __name__ == "__main__":

    import glob

    files = glob.glob("../data/silver/*/*/*/crypto.parquet")

    if not files:
        raise FileNotFoundError("No silver files found")

    df_silver = pd.concat(
        [pd.read_parquet(f) for f in files],
        ignore_index=True
    )

    dim_crypto, dim_date, fact = gold_pipeline(df_silver)

    print("\n📅 DIM_DATE SAMPLE")
    print(dim_date.head())

    print("\n📊 FACT SAMPLE")
    print(fact.head())
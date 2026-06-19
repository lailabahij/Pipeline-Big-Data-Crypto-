import json
import pandas as pd
from datetime import datetime
from minio import Minio
from minio.error import S3Error
from io import BytesIO
import os
import logging

# =====================================
# LOGGING CONFIG
# =====================================
logging.basicConfig(
    filename="pipeline_silver.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# =====================================
# 1. Connexion MinIO
# =====================================
def get_minio_client():
    logger.info("Connexion à MinIO")
    return Minio(
          "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

# =====================================
# 2. Lecture Bronze
# =====================================
def get_latest_bronze_file(client):

    objects = list(
        client.list_objects("crypto-bronze", recursive=True)
    )

    if not objects:
        logger.error("Aucun fichier Bronze trouvé")
        raise Exception("Aucun fichier Bronze trouvé")

    latest_file = sorted(
        objects,
        key=lambda x: x.last_modified,
        reverse=True
    )[0]

    logger.info(f"Fichier Bronze sélectionné : {latest_file.object_name}")

    return latest_file.object_name


def read_all_bronze_files(client):

    bucket_name = "crypto-bronze"

    objects = list(
        client.list_objects(bucket_name, recursive=True)
    )

    if not objects:
        raise Exception("Aucun fichier Bronze trouvé")

    all_data = []

    print("\n📂 Fichiers Bronze trouvés :")

    for obj in sorted(objects, key=lambda x: x.object_name):

        print(obj.object_name)

        response = client.get_object(
            bucket_name,
            obj.object_name
        )

        content = json.loads(response.read())

        if "data" in content:
            all_data.extend(content["data"])

    print(f"\n📊 Total lignes récupérées : {len(all_data)}")

    return {"data": all_data}

# =====================================
# 3. Transformation Silver
# =====================================
# =====================================
# 3. Transformation Silver
# =====================================
def transform_data(raw_data):

    crypto_data = raw_data["data"]
    df = pd.DataFrame(crypto_data)

    logger.info(f"Nombre de lignes initiales : {len(df)}")

    # Normalisation colonnes
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    columns_to_keep = [
        "id","symbol","name","current_price","market_cap",
        "market_cap_rank","total_volume","high_24h","low_24h",
        "price_change_24h","price_change_percentage_24h",
        "circulating_supply","max_supply","ath","last_updated"
    ]

    df = df[columns_to_keep]

    # Types string
    for col in ["id", "symbol", "name"]:
        df[col] = df[col].astype(str)

    # Types numeric
    numeric_cols = [
        "current_price","market_cap","market_cap_rank",
        "total_volume","high_24h","low_24h",
        "price_change_24h","price_change_percentage_24h",
        "circulating_supply","max_supply","ath"
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Date conversion
    df["last_updated"] = pd.to_datetime(df["last_updated"], errors="coerce")
    print(df["last_updated"].min())
    print(df["last_updated"].max())
    print(df["last_updated"].nunique())
    # =====================================
    # Features temporelles
    # =====================================

    # Conserver le timestamp complet
    df["timestamp"] = df["last_updated"]

    # Extraire les dimensions temporelles
    df["date"] = df["timestamp"].dt.date
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month
    df["day"] = df["timestamp"].dt.day
    df["hour"] = df["timestamp"].dt.hour
    df["minute"] = df["timestamp"].dt.minute
    df["week"] = df["timestamp"].dt.isocalendar().week.astype(int)
    df["quarter"] = df["timestamp"].dt.quarter

    # =====================================
    # Missing values check
    # =====================================
    missing = df.isnull().sum()
    logger.info(f"Valeurs manquantes:\n{missing[missing > 0]}")

    rows_before = len(df)

    # =====================================
    # Skewness analysis
    # =====================================
    skew_value = df["max_supply"].dropna().skew()

    logger.info(f"Skewness max_supply = {skew_value}")

    if abs(skew_value) > 1:
        logger.warning("Distribution très asymétrique → MEDIAN recommandée")
    else:
        logger.info("Distribution normale → MEAN possible")

    # =====================================
    # Fill missing values
    # =====================================
    df["max_supply"] = df["max_supply"].fillna(df["max_supply"].median())

    # Drop remaining NaN
    df = df.dropna()

    rows_after = len(df)

    logger.info(f"Lignes supprimées : {rows_before - rows_after}")
    logger.info(f"Lignes restantes : {rows_after}")

    df["collection_date"] = pd.Timestamp.now()

    logger.info("Transformation Silver terminée")
    print("Dates uniques :", df["date"].nunique())
    print("Heures uniques :", df["hour"].nunique())
    print("Minutes uniques :", df["minute"].nunique())
    print("Dates :", sorted(df["date"].unique()))

    print("Heures :", sorted(df["hour"].unique()))

    print("Minutes :", sorted(df["minute"].unique()))
    print(
        df[["date", "hour", "minute"]]
        .drop_duplicates()
        .sort_values(["hour", "minute"])
    )
    #################################
    print("\nDATES SILVER")

    print(
        sorted(
            df["timestamp"].dt.date.unique()
        )
    )

    print("\nNB DATES UNIQUES")

    print(
        df["timestamp"].dt.date.nunique()
    )
    return df
    
# =====================================
# 4. Sauvegarde Silver
# =====================================
def save_to_silver(client, df):

    bucket_name = "crypto-silver"

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        logger.info("Bucket crypto-silver créé")

    today = datetime.now()

    local_path = (
        f"../data/silver/{today.year}/{today.month:02d}/{today.day:02d}"
    )

    os.makedirs(local_path, exist_ok=True)

    local_file = f"{local_path}/crypto.parquet"

    df.to_parquet(local_file, index=False, engine="pyarrow")
    logger.info(f"Fichier local sauvegardé : {local_file}")

    object_name = f"{today.year}/{today.month:02d}/{today.day:02d}/crypto.parquet"

    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    client.put_object(
        bucket_name,
        object_name,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )

    logger.info(f"Fichier MinIO sauvegardé : {object_name}")

# =====================================
# 5. Pipeline principal
# =====================================
def main():

    logger.info("🚀 Début pipeline Silver")

    try:
        client = get_minio_client()

        raw_data = read_all_bronze_files(client)

        df = transform_data(raw_data)

        save_to_silver(client, df)

        logger.info("✅ Pipeline Silver terminé avec succès")

    except S3Error as e:
        logger.error(f"Erreur MinIO : {e}")

    except Exception as e:
        logger.error(f"Erreur générale : {e}")

# =====================================
# EXECUTION
# =====================================
if __name__ == "__main__":
    main()
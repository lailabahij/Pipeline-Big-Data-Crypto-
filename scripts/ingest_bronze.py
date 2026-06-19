import requests
import json
import os
from datetime import datetime
from minio import Minio
from minio.error import S3Error


# =====================================
# 1. Collecte des données CoinGecko
# =====================================

def fetch_crypto_data():
    """Collecte des données depuis CoinGecko"""

    url = "https://api.coingecko.com/api/v3/coins/markets"

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "sparkline": "false"
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()

        # ✔ Observation : vérifier si données vides
        if not data:
            print("⚠ Aucune donnée récupérée depuis l'API.")
            return None

        return data

    except requests.exceptions.Timeout:
        print("❌ Erreur : délai d'attente dépassé.")
        return None

    except requests.exceptions.HTTPError as e:
        print(f"❌ Erreur HTTP : {e}")
        return None

    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur de connexion : {e}")
        return None


# =====================================
# 2. Affichage infos
# =====================================

def display_data_info(data):
    """Affiche le nombre de cryptos récupérées"""
    print(f"📊 Nombre de cryptos récupérées : {len(data)}")


# =====================================
# 3. Création dossier Bronze local
# =====================================

def create_bronze_path():
    """Crée le dossier Bronze local"""

    today = datetime.now()

    path = (
        f"../data/bronze/"
        f"{today.year}/"
        f"{today.month:02d}/"
        f"{today.day:02d}"
    )

    os.makedirs(path, exist_ok=True)

    return path


# =====================================
# 4. Sauvegarde JSON local
# =====================================

def save_to_bronze(data, path):
    """Sauvegarde le JSON brut localement"""

    file_path = f"{path}/raw.json"

    # ✔ Observation : ajouter timestamp (bonne pratique data engineering)
    enriched_data = {
        "ingestion_time": datetime.now().isoformat(),
        "data": data
    }

    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(enriched_data, file, indent=4)

    print(f"💾 Fichier local créé : {file_path}")

    return file_path


# =====================================
# 5. Connexion MinIO
# =====================================

def get_minio_client():
    """Connexion au serveur MinIO"""

    client = Minio(
          "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    return client


# =====================================
# 6. Création bucket
# =====================================

def create_bucket(client):
    """Crée le bucket crypto-bronze si nécessaire"""

    bucket_name = "crypto-bronze"

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"🪣 Bucket créé : {bucket_name}")
    else:
        print(f"🪣 Bucket existant : {bucket_name}")

    return bucket_name


# =====================================
# 7. Upload vers MinIO
# =====================================

def upload_to_minio(client, bucket_name, local_file):
    """Upload du fichier JSON dans MinIO"""

    today = datetime.now()

    object_name = (
        f"{today.year}/"
        f"{today.month:02d}/"
        f"{today.day:02d}/"
        f"raw.json"
    )

    client.fput_object(
        bucket_name=bucket_name,
        object_name=object_name,
        file_path=local_file,
        content_type="application/json"
    )

    print(f"☁️ Fichier envoyé vers MinIO : {bucket_name}/{object_name}")


# =====================================
# 8. Pipeline principal
# =====================================

def main():

    print("🚀 Début ingestion Bronze...")

    # 1. API ingestion
    data = fetch_crypto_data()

    if data is None:
        print("❌ Aucune donnée récupérée. Arrêt pipeline.")
        return

    # 2. Info dataset
    display_data_info(data)

    # 3. Création dossier local Bronze
    bronze_path = create_bronze_path()

    # 4. Sauvegarde locale
    local_file = save_to_bronze(data, bronze_path)

    try:
        # 5. Connexion MinIO
        client = get_minio_client()

        # 6. Création bucket
        bucket_name = create_bucket(client)

        # 7. Upload vers MinIO
        upload_to_minio(client, bucket_name, local_file)

    except S3Error as e:
        # ✔ Observation : logs plus détaillés
        print(f"❌ Erreur MinIO : {e.code} - {e.message}")

    print("✅ Ingestion Bronze terminée avec succès.")


# =====================================
# Exécution
# =====================================

if __name__ == "__main__":
    main()
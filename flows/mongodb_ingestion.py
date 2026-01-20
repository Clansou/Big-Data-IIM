"""
Pipeline d'ingestion Gold -> MongoDB
Lit les fichiers Parquet du bucket Gold et les charge dans MongoDB
"""
import pandas as pd
from io import BytesIO
from pathlib import Path
from datetime import datetime
import sys
import time
from prefect import flow, task

sys.path.append(str(Path(__file__).parent.parent))
from utils.config import (
    BUCKET_GOLD,
    get_minio_client,
    get_mongodb_database,
    COLLECTION_CLIENTS,
    COLLECTION_PRODUCTS,
    COLLECTION_MONTHLY_SALES,
    COLLECTION_METADATA
)


def read_parquet_from_minio(bucket: str, object_name: str) -> pd.DataFrame:
    """Lire un fichier Parquet depuis MinIO"""
    client = get_minio_client()
    response = client.get_object(bucket, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    return pd.read_parquet(BytesIO(data))


@task(name="load_clients_to_mongodb", retries=2)
def load_clients_to_mongodb() -> dict:
    """Charger les données clients du Gold vers MongoDB"""
    start_time = time.time()

    df = read_parquet_from_minio(BUCKET_GOLD, "gold_client_summary.parquet")

    db = get_mongodb_database()
    collection = db[COLLECTION_CLIENTS]

    # Supprimer les anciennes données
    collection.delete_many({})

    # Convertir le DataFrame en liste de dictionnaires
    records = df.to_dict(orient="records")

    # Insérer les nouvelles données
    if records:
        collection.insert_many(records)

    # Créer des index pour les requêtes fréquentes
    collection.create_index("client_id", unique=True)
    collection.create_index("country")
    collection.create_index("total_achats")

    elapsed_time = time.time() - start_time

    print(f"Loaded {len(records)} clients to MongoDB in {elapsed_time:.2f}s")
    return {
        "collection": COLLECTION_CLIENTS,
        "count": len(records),
        "elapsed_time": elapsed_time
    }


@task(name="load_products_to_mongodb", retries=2)
def load_products_to_mongodb() -> dict:
    """Charger les statistiques produits du Gold vers MongoDB"""
    start_time = time.time()

    df = read_parquet_from_minio(BUCKET_GOLD, "gold_product_stats.parquet")

    db = get_mongodb_database()
    collection = db[COLLECTION_PRODUCTS]

    # Supprimer les anciennes données
    collection.delete_many({})

    # Convertir le DataFrame en liste de dictionnaires
    records = df.to_dict(orient="records")

    # Insérer les nouvelles données
    if records:
        collection.insert_many(records)

    # Créer des index
    collection.create_index("product", unique=True)
    collection.create_index("chiffre_affaires")

    elapsed_time = time.time() - start_time

    print(f"Loaded {len(records)} products to MongoDB in {elapsed_time:.2f}s")
    return {
        "collection": COLLECTION_PRODUCTS,
        "count": len(records),
        "elapsed_time": elapsed_time
    }


@task(name="load_monthly_sales_to_mongodb", retries=2)
def load_monthly_sales_to_mongodb() -> dict:
    """Charger les ventes mensuelles du Gold vers MongoDB"""
    start_time = time.time()

    df = read_parquet_from_minio(BUCKET_GOLD, "gold_monthly_sales.parquet")

    db = get_mongodb_database()
    collection = db[COLLECTION_MONTHLY_SALES]

    # Supprimer les anciennes données
    collection.delete_many({})

    # Convertir le DataFrame en liste de dictionnaires
    records = df.to_dict(orient="records")

    # Insérer les nouvelles données
    if records:
        collection.insert_many(records)

    # Créer des index
    collection.create_index("mois", unique=True)

    elapsed_time = time.time() - start_time

    print(f"Loaded {len(records)} monthly records to MongoDB in {elapsed_time:.2f}s")
    return {
        "collection": COLLECTION_MONTHLY_SALES,
        "count": len(records),
        "elapsed_time": elapsed_time
    }


@task(name="update_metadata")
def update_metadata(results: list[dict]) -> dict:
    """Mettre à jour les métadonnées de refresh dans MongoDB"""
    db = get_mongodb_database()
    collection = db[COLLECTION_METADATA]

    total_time = sum(r["elapsed_time"] for r in results)
    total_records = sum(r["count"] for r in results)

    metadata = {
        "last_refresh": datetime.utcnow(),
        "total_refresh_time_seconds": round(total_time, 2),
        "total_records_loaded": total_records,
        "collections_refreshed": [r["collection"] for r in results],
        "details": results
    }

    # Upsert les métadonnées
    collection.update_one(
        {"_id": "refresh_info"},
        {"$set": metadata},
        upsert=True
    )

    print(f"Metadata updated: {total_records} records in {total_time:.2f}s")
    return metadata


@flow(name="MongoDB Ingestion Flow")
def mongodb_ingestion_flow() -> dict:
    """
    Flow principal pour charger les données Gold dans MongoDB.
    Calcule et stocke le temps de refresh.
    """
    print("=== Début de l'ingestion Gold -> MongoDB ===")

    flow_start_time = time.time()

    # Charger les 3 collections
    clients_result = load_clients_to_mongodb()
    products_result = load_products_to_mongodb()
    monthly_result = load_monthly_sales_to_mongodb()

    results = [clients_result, products_result, monthly_result]

    # Mettre à jour les métadonnées avec le temps de refresh
    metadata = update_metadata(results)

    flow_elapsed_time = time.time() - flow_start_time

    print("=== Ingestion MongoDB terminée ===")
    print(f"Temps total du flow: {flow_elapsed_time:.2f}s")

    return {
        "status": "success",
        "flow_elapsed_time": round(flow_elapsed_time, 2),
        "metadata": metadata
    }


def get_refresh_info() -> dict:
    """Récupérer les informations de refresh depuis MongoDB"""
    db = get_mongodb_database()
    collection = db[COLLECTION_METADATA]

    info = collection.find_one({"_id": "refresh_info"})
    if info:
        info.pop("_id", None)
    return info


if __name__ == "__main__":
    result = mongodb_ingestion_flow()
    print(f"\nRésultat: {result}")

    print("\n--- Informations de refresh ---")
    refresh_info = get_refresh_info()
    if refresh_info:
        print(f"Dernier refresh: {refresh_info['last_refresh']}")
        print(f"Temps de refresh: {refresh_info['total_refresh_time_seconds']}s")
        print(f"Records chargés: {refresh_info['total_records_loaded']}")

import pandas as pd
from io import BytesIO
from pathlib import Path
import sys
from prefect import flow, task

# Ajouter le dossier parent au PYTHONPATH
sys.path.append(str(Path(__file__).parent.parent))
from utils.config import BUCKET_SILVER, BUCKET_GOLD, get_minio_client


def read_parquet_from_minio(bucket: str, object_name: str) -> pd.DataFrame:
    """Lire un fichier Parquet depuis MinIO"""
    client = get_minio_client()
    response = client.get_object(bucket, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    return pd.read_parquet(BytesIO(data))


def write_parquet_to_minio(bucket: str, object_name: str, df: pd.DataFrame):
    """Écrire un DataFrame en Parquet vers MinIO"""
    client = get_minio_client()
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)
    
    client.put_object(bucket, object_name, parquet_buffer, length=len(parquet_buffer.getvalue()))
    print(f"Saved {len(df)} rows to {bucket}/{object_name}")


@task(name="build_client_summary")
def build_client_summary() -> str:
    """Créer un résumé des clients avec leurs statistiques d'achats"""
    clients = read_parquet_from_minio(BUCKET_SILVER, "clients.parquet")
    purchases = read_parquet_from_minio(BUCKET_SILVER, "purchases.parquet")
    
    # Agrégations simples par client
    stats_purchases = purchases.groupby("client_id").agg({
        "amount": ["sum", "mean", "count"],
        "date_purchase": ["min", "max"]
    }).round(2)
    
    # Aplatir les colonnes multi-niveaux
    stats_purchases.columns = ["total_achats", "panier_moyen", "nb_achats", "premier_achat", "dernier_achat"]
    stats_purchases = stats_purchases.reset_index()
    
    # Jointure avec les clients
    summary = clients.merge(stats_purchases, on="client_id", how="left")
    summary = summary.fillna(0)
    
    write_parquet_to_minio(BUCKET_GOLD, "gold_client_summary.parquet", summary)
    return "gold_client_summary.parquet"


@task(name="build_product_stats")
def build_product_stats() -> str:
    """Créer des statistiques par produit"""
    purchases = read_parquet_from_minio(BUCKET_SILVER, "purchases.parquet")
    
    # Statistiques par produit
    product_stats = purchases.groupby("product").agg({
        "amount": ["sum", "mean", "count"]
    }).round(2)
    
    # Aplatir les colonnes
    product_stats.columns = ["chiffre_affaires", "prix_moyen", "nb_ventes"]
    product_stats = product_stats.reset_index()
    product_stats = product_stats.sort_values("chiffre_affaires", ascending=False)
    
    write_parquet_to_minio(BUCKET_GOLD, "gold_product_stats.parquet", product_stats)
    return "gold_product_stats.parquet"


@task(name="build_monthly_sales")
def build_monthly_sales() -> str:
    """Créer les ventes mensuelles"""
    purchases = read_parquet_from_minio(BUCKET_SILVER, "purchases.parquet")
    
    # Convertir la date et extraire le mois
    purchases["date_purchase"] = pd.to_datetime(purchases["date_purchase"])
    purchases["mois"] = purchases["date_purchase"].dt.to_period("M").astype(str)
    
    # Agrégations par mois
    monthly = purchases.groupby("mois").agg({
        "amount": ["sum", "mean", "count"]
    }).round(2)
    
    # Aplatir les colonnes
    monthly.columns = ["chiffre_affaires", "panier_moyen", "nb_achats"]
    monthly = monthly.reset_index()
    monthly = monthly.sort_values("mois")
    
    write_parquet_to_minio(BUCKET_GOLD, "gold_monthly_sales.parquet", monthly)
    return "gold_monthly_sales.parquet"


@flow(name="Simple Gold Flow")
def simple_gold_flow() -> dict:
    """Flow simplifié pour créer les tables gold"""
    print("=== Génération des tables GOLD ===")
    
    # Créer les 3 tables principales
    client_summary = build_client_summary()
    product_stats = build_product_stats()  
    monthly_sales = build_monthly_sales()
    
    print("=== Génération GOLD terminée ===")
    
    return {
        "client_summary": client_summary,
        "product_stats": product_stats,
        "monthly_sales": monthly_sales
    }


if __name__ == "__main__":
    result = simple_gold_flow()
    print(f"Tables gold générées: {result}")
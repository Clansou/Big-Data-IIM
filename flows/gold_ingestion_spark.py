"""
Gold Ingestion avec PySpark
Version parallèle à gold_ingestion.py (Pandas) pour comparaison de performance
"""
from pathlib import Path
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from prefect import flow, task

# Ajouter le dossier parent au PYTHONPATH
sys.path.append(str(Path(__file__).parent.parent))

from utils.config import (
    BUCKET_SILVER, BUCKET_GOLD,
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
)


def get_spark_session() -> SparkSession:
    """
    Create and configure a SparkSession with MinIO (S3) support.
    """
    spark = (SparkSession.builder
        .appName("GoldIngestion")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_parquet_from_minio(spark: SparkSession, bucket: str, object_name: str) -> DataFrame:
    """Lire un fichier Parquet depuis MinIO avec Spark"""
    path = f"s3a://{bucket}/{object_name}"
    return spark.read.parquet(path)


def write_parquet_to_minio(df: DataFrame, bucket: str, object_name: str):
    """Écrire un DataFrame en Parquet vers MinIO avec Spark"""
    path = f"s3a://{bucket}/{object_name}"
    df.write.mode("overwrite").parquet(path)
    print(f"Saved {df.count()} rows to {bucket}/{object_name}")


@task(name="spark_build_client_summary")
def build_client_summary(spark: SparkSession) -> str:
    """Créer un résumé des clients avec leurs statistiques d'achats"""
    clients = read_parquet_from_minio(spark, BUCKET_SILVER, "clients_spark.parquet")
    purchases = read_parquet_from_minio(spark, BUCKET_SILVER, "purchases_spark.parquet")

    # Agrégations par client
    stats_purchases = purchases.groupBy("client_id").agg(
        F.round(F.sum("amount"), 2).alias("total_achats"),
        F.round(F.avg("amount"), 2).alias("panier_moyen"),
        F.count("*").alias("nb_achats"),
        F.min("date_purchase").alias("premier_achat"),
        F.max("date_purchase").alias("dernier_achat")
    )

    # Jointure avec les clients
    summary = clients.join(stats_purchases, on="client_id", how="left")
    summary = summary.na.fill(0)

    write_parquet_to_minio(summary, BUCKET_GOLD, "gold_client_summary_spark.parquet")
    return "gold_client_summary_spark.parquet"


@task(name="spark_build_product_stats")
def build_product_stats(spark: SparkSession) -> str:
    """Créer des statistiques par produit"""
    purchases = read_parquet_from_minio(spark, BUCKET_SILVER, "purchases_spark.parquet")

    # Statistiques par produit
    product_stats = purchases.groupBy("product").agg(
        F.round(F.sum("amount"), 2).alias("chiffre_affaires"),
        F.round(F.avg("amount"), 2).alias("prix_moyen"),
        F.count("*").alias("nb_ventes")
    )

    product_stats = product_stats.orderBy(F.desc("chiffre_affaires"))

    write_parquet_to_minio(product_stats, BUCKET_GOLD, "gold_product_stats_spark.parquet")
    return "gold_product_stats_spark.parquet"


@task(name="spark_build_monthly_sales")
def build_monthly_sales(spark: SparkSession) -> str:
    """Créer les ventes mensuelles"""
    purchases = read_parquet_from_minio(spark, BUCKET_SILVER, "purchases_spark.parquet")

    # Extraire le mois au format YYYY-MM
    purchases_with_month = purchases.withColumn(
        "mois",
        F.date_format(F.col("date_purchase"), "yyyy-MM")
    )

    # Agrégations par mois
    monthly = purchases_with_month.groupBy("mois").agg(
        F.round(F.sum("amount"), 2).alias("chiffre_affaires"),
        F.round(F.avg("amount"), 2).alias("panier_moyen"),
        F.count("*").alias("nb_achats")
    )

    monthly = monthly.orderBy("mois")

    write_parquet_to_minio(monthly, BUCKET_GOLD, "gold_monthly_sales_spark.parquet")
    return "gold_monthly_sales_spark.parquet"


@flow(name="Simple Gold Flow (Spark)")
def simple_gold_flow_spark() -> dict:
    """Flow simplifié pour créer les tables gold avec Spark"""
    print("=== Génération des tables GOLD (SPARK) ===")

    spark = get_spark_session()

    try:
        client_summary = build_client_summary(spark)
        product_stats = build_product_stats(spark)
        monthly_sales = build_monthly_sales(spark)

        print("=== Génération GOLD (SPARK) terminée ===")

        return {
            "client_summary": client_summary,
            "product_stats": product_stats,
            "monthly_sales": monthly_sales
        }
    finally:
        spark.stop()


if __name__ == "__main__":
    result = simple_gold_flow_spark()
    print(f"Tables gold générées: {result}")

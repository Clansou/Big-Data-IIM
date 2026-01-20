"""
Silver Ingestion avec PySpark
Version parallèle à silver_ingestion.py (Pandas) pour comparaison de performance
"""
from datetime import datetime
from typing import Dict, Tuple
from pathlib import Path
import sys
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, TimestampType
from prefect import flow, task

# Ajouter le dossier parent au PYTHONPATH
sys.path.append(str(Path(__file__).parent.parent))

from utils.config import (
    BUCKET_BRONZE, BUCKET_SILVER,
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
)


def get_spark_session() -> SparkSession:
    """
    Create and configure a SparkSession with MinIO (S3) support.
    """
    spark = (SparkSession.builder
        .appName("SilverIngestion")
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


@task(name="spark_load_from_bronze", retries=2)
def load_data_from_bronze(spark: SparkSession, object_name: str) -> DataFrame:
    """
    Load CSV data from bronze bucket into Spark DataFrame.
    """
    path = f"s3a://{BUCKET_BRONZE}/{object_name}"
    df = spark.read.csv(path, header=True, inferSchema=True)
    print(f"Loaded {df.count()} rows from {object_name}")
    return df


@task(name="spark_quality_check_initial")
def quality_check_initial(df: DataFrame, dataset_name: str) -> Dict:
    """
    Perform initial quality checks on raw data.
    """
    total_rows = df.count()
    total_columns = len(df.columns)

    # Count duplicates
    duplicates = total_rows - df.dropDuplicates().count()

    # Count missing values per column
    missing_values = {}
    for col_name in df.columns:
        null_count = df.filter(F.col(col_name).isNull()).count()
        missing_values[col_name] = null_count

    # Get data types
    dtypes = {field.name: str(field.dataType) for field in df.schema.fields}

    metrics = {
        "dataset": dataset_name,
        "total_rows": total_rows,
        "total_columns": total_columns,
        "duplicates": duplicates,
        "missing_values": missing_values,
        "dtypes": dtypes
    }

    print(f"\n=== Quality Check: {dataset_name} ===")
    print(f"Total rows: {metrics['total_rows']}")
    print(f"Duplicates: {metrics['duplicates']}")
    print(f"Missing values per column:")
    for col, missing in metrics['missing_values'].items():
        if missing > 0:
            pct = missing / total_rows * 100 if total_rows > 0 else 0
            print(f"  - {col}: {missing} ({pct:.2f}%)")

    return metrics


@task(name="spark_clean_clients_data")
def clean_clients_data(df: DataFrame) -> Tuple[DataFrame, Dict]:
    """
    Clean and transform clients data with PySpark.
    """
    initial_count = df.count()
    df_clean = df

    stats = {
        "initial_rows": initial_count,
        "removed_null_critical": 0,
        "removed_duplicates": 0,
        "removed_invalid_dates": 0,
        "removed_invalid_emails": 0,
        "final_rows": 0
    }

    # 1. Remove rows with null values in critical columns
    critical_cols = ["client_id", "email"]
    before = df_clean.count()
    df_clean = df_clean.na.drop(subset=critical_cols)
    stats["removed_null_critical"] = before - df_clean.count()

    # 2. Fill missing values in non-critical columns
    df_clean = df_clean.na.fill({"name": "Unknown", "country": "Unknown"})

    # 3. Standardize date format and remove invalid dates
    before = df_clean.count()
    df_clean = df_clean.withColumn(
        "date_inscription",
        F.to_timestamp(F.col("date_inscription"))
    )
    df_clean = df_clean.na.drop(subset=["date_inscription"])
    stats["removed_invalid_dates"] = before - df_clean.count()

    # Remove future dates
    df_clean = df_clean.filter(F.col("date_inscription") <= F.current_timestamp())

    # 4. Clean and validate email addresses
    before = df_clean.count()
    df_clean = df_clean.withColumn("email", F.lower(F.trim(F.col("email"))))
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    df_clean = df_clean.filter(F.col("email").rlike(email_pattern))
    stats["removed_invalid_emails"] = before - df_clean.count()

    # 5. Normalize data types
    df_clean = df_clean.withColumn("client_id", F.col("client_id").cast(IntegerType()))
    df_clean = df_clean.withColumn("name", F.trim(F.col("name")))
    df_clean = df_clean.withColumn("country", F.trim(F.col("country")))

    # 6. Remove duplicates
    before = df_clean.count()
    df_clean = df_clean.dropDuplicates(["client_id"])
    df_clean = df_clean.dropDuplicates(["email"])
    stats["removed_duplicates"] = before - df_clean.count()

    # 7. Sort by client_id
    df_clean = df_clean.orderBy("client_id")

    stats["final_rows"] = df_clean.count()
    stats["data_loss_percentage"] = (initial_count - stats["final_rows"]) / initial_count * 100 if initial_count > 0 else 0

    print(f"\n=== Clients Cleaning Stats ===")
    print(f"Initial rows: {stats['initial_rows']}")
    print(f"Removed (null critical columns): {stats['removed_null_critical']}")
    print(f"Removed (invalid dates): {stats['removed_invalid_dates']}")
    print(f"Removed (invalid emails): {stats['removed_invalid_emails']}")
    print(f"Removed (duplicates): {stats['removed_duplicates']}")
    print(f"Final rows: {stats['final_rows']}")
    print(f"Data loss: {stats['data_loss_percentage']:.2f}%")

    return df_clean, stats


@task(name="spark_clean_purchases_data")
def clean_purchases_data(df: DataFrame, valid_client_ids: list) -> Tuple[DataFrame, Dict]:
    """
    Clean and transform purchases data with PySpark.
    """
    initial_count = df.count()
    df_clean = df

    stats = {
        "initial_rows": initial_count,
        "removed_null_critical": 0,
        "removed_duplicates": 0,
        "removed_invalid_dates": 0,
        "removed_invalid_amounts": 0,
        "removed_invalid_clients": 0,
        "removed_outliers": 0,
        "final_rows": 0
    }

    # 1. Remove rows with null values in critical columns
    critical_cols = ["purchase_id", "client_id", "amount", "date_purchase"]
    before = df_clean.count()
    df_clean = df_clean.na.drop(subset=critical_cols)
    stats["removed_null_critical"] = before - df_clean.count()

    # 2. Fill missing product names
    df_clean = df_clean.na.fill({"product": "Unknown Product"})

    # 3. Standardize date format and remove invalid dates
    before = df_clean.count()
    df_clean = df_clean.withColumn(
        "date_purchase",
        F.to_timestamp(F.col("date_purchase"))
    )
    df_clean = df_clean.na.drop(subset=["date_purchase"])
    stats["removed_invalid_dates"] = before - df_clean.count()

    # Remove future dates
    df_clean = df_clean.filter(F.col("date_purchase") <= F.current_timestamp())

    # 4. Normalize data types
    df_clean = df_clean.withColumn("purchase_id", F.col("purchase_id").cast(IntegerType()))
    df_clean = df_clean.withColumn("client_id", F.col("client_id").cast(IntegerType()))
    df_clean = df_clean.withColumn("amount", F.col("amount").cast(DoubleType()))
    df_clean = df_clean.withColumn("product", F.trim(F.col("product")))

    # 5. Remove invalid amounts (negative or zero)
    before = df_clean.count()
    df_clean = df_clean.filter(F.col("amount") > 0)
    stats["removed_invalid_amounts"] = before - df_clean.count()

    # 6. Remove purchases with invalid client_id (referential integrity)
    before = df_clean.count()
    df_clean = df_clean.filter(F.col("client_id").isin(valid_client_ids))
    stats["removed_invalid_clients"] = before - df_clean.count()

    # 7. Remove statistical outliers (amounts > Q3 + 3*IQR)
    before = df_clean.count()
    quantiles = df_clean.approxQuantile("amount", [0.25, 0.75], 0.01)
    if len(quantiles) == 2:
        Q1, Q3 = quantiles
        IQR = Q3 - Q1
        lower_bound = Q1 - 3 * IQR
        upper_bound = Q3 + 3 * IQR
        print(f"Amount bounds: [{lower_bound:.2f}, {upper_bound:.2f}]")
        df_clean = df_clean.filter(
            (F.col("amount") >= lower_bound) & (F.col("amount") <= upper_bound)
        )
    stats["removed_outliers"] = before - df_clean.count()

    # 8. Remove duplicates
    before = df_clean.count()
    df_clean = df_clean.dropDuplicates(["purchase_id"])
    stats["removed_duplicates"] = before - df_clean.count()

    # 9. Sort by purchase_id
    df_clean = df_clean.orderBy("purchase_id")

    stats["final_rows"] = df_clean.count()
    stats["data_loss_percentage"] = (initial_count - stats["final_rows"]) / initial_count * 100 if initial_count > 0 else 0

    print(f"\n=== Purchases Cleaning Stats ===")
    print(f"Initial rows: {stats['initial_rows']}")
    print(f"Removed (null critical columns): {stats['removed_null_critical']}")
    print(f"Removed (invalid dates): {stats['removed_invalid_dates']}")
    print(f"Removed (invalid amounts): {stats['removed_invalid_amounts']}")
    print(f"Removed (invalid client_id): {stats['removed_invalid_clients']}")
    print(f"Removed (outliers): {stats['removed_outliers']}")
    print(f"Removed (duplicates): {stats['removed_duplicates']}")
    print(f"Final rows: {stats['final_rows']}")
    print(f"Data loss: {stats['data_loss_percentage']:.2f}%")

    return df_clean, stats


@task(name="spark_quality_check_final")
def quality_check_final(df: DataFrame, dataset_name: str) -> Dict:
    """
    Perform final quality checks on cleaned data.
    """
    total_rows = df.count()
    duplicates = total_rows - df.dropDuplicates().count()

    # Total missing values
    missing_total = 0
    for col_name in df.columns:
        missing_total += df.filter(F.col(col_name).isNull()).count()

    dtypes = {field.name: str(field.dataType) for field in df.schema.fields}

    metrics = {
        "dataset": dataset_name,
        "total_rows": total_rows,
        "duplicates": duplicates,
        "missing_values": missing_total,
        "dtypes": dtypes
    }

    print(f"\n=== Final Quality Check: {dataset_name} ===")
    print(f"Total rows: {metrics['total_rows']}")
    print(f"Duplicates: {metrics['duplicates']}")
    print(f"Total missing values: {metrics['missing_values']}")
    print(f"Data types: {metrics['dtypes']}")

    assert metrics["duplicates"] == 0, "Duplicates found in cleaned data!"

    return metrics


@task(name="spark_save_to_silver", retries=2)
def save_to_silver(df: DataFrame, object_name: str) -> str:
    """
    Save cleaned DataFrame to silver bucket in Parquet format.
    """
    path = f"s3a://{BUCKET_SILVER}/{object_name}.parquet"

    # Write as Parquet (overwrite mode)
    df.write.mode("overwrite").parquet(path)

    print(f"Saved {df.count()} rows to {BUCKET_SILVER}/{object_name}.parquet")
    return f"{object_name}.parquet"


@flow(name="Silver Transformation Flow (Spark)")
def silver_transformation_flow_spark() -> Dict:
    """
    Main flow: Transform bronze data to silver using PySpark.
    """
    # Initialize Spark
    spark = get_spark_session()

    try:
        # === CLIENTS TRANSFORMATION ===
        print("\n" + "="*50)
        print("CLIENTS TRANSFORMATION (SPARK)")
        print("="*50)

        df_clients_raw = load_data_from_bronze(spark, "clients.csv")
        clients_initial_metrics = quality_check_initial(df_clients_raw, "clients")
        df_clients_clean, clients_stats = clean_clients_data(df_clients_raw)
        clients_final_metrics = quality_check_final(df_clients_clean, "clients")
        clients_silver = save_to_silver(df_clients_clean, "clients_spark")

        # Get valid client IDs for purchases validation
        valid_client_ids = [row.client_id for row in df_clients_clean.select("client_id").collect()]

        # === PURCHASES TRANSFORMATION ===
        print("\n" + "="*50)
        print("PURCHASES TRANSFORMATION (SPARK)")
        print("="*50)

        df_purchases_raw = load_data_from_bronze(spark, "purchases.csv")
        purchases_initial_metrics = quality_check_initial(df_purchases_raw, "purchases")
        df_purchases_clean, purchases_stats = clean_purchases_data(df_purchases_raw, valid_client_ids)
        purchases_final_metrics = quality_check_final(df_purchases_clean, "purchases")
        purchases_silver = save_to_silver(df_purchases_clean, "purchases_spark")

        print("\n" + "="*50)
        print("TRANSFORMATION SUMMARY (SPARK)")
        print("="*50)

        result = {
            "clients": {
                "silver_object": clients_silver,
                "initial_metrics": clients_initial_metrics,
                "cleaning_stats": clients_stats,
                "final_metrics": clients_final_metrics
            },
            "purchases": {
                "silver_object": purchases_silver,
                "initial_metrics": purchases_initial_metrics,
                "cleaning_stats": purchases_stats,
                "final_metrics": purchases_final_metrics
            },
            "timestamp": datetime.now().isoformat()
        }

        print(f"\nClients: {clients_stats['initial_rows']} → {clients_stats['final_rows']} rows")
        print(f"Purchases: {purchases_stats['initial_rows']} → {purchases_stats['final_rows']} rows")
        print(f"\nSilver transformation (Spark) complete!")

        return result

    finally:
        spark.stop()


if __name__ == "__main__":
    result = silver_transformation_flow_spark()

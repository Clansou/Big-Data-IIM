"""Configuration pour MinIO et MongoDB"""
import os
from pathlib import Path

from dotenv import load_dotenv
from minio import Minio
from pymongo import MongoClient

load_dotenv()

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "False").lower() == "true"

# MongoDB configuration
MONGODB_HOST = os.getenv("MONGODB_HOST", "localhost")
MONGODB_PORT = int(os.getenv("MONGODB_PORT", "27017"))
MONGODB_USERNAME = os.getenv("MONGODB_USERNAME", "admin")
MONGODB_PASSWORD = os.getenv("MONGODB_PASSWORD", "admin")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "analytics")

# Collections MongoDB
COLLECTION_CLIENTS = "clients"
COLLECTION_PRODUCTS = "products"
COLLECTION_MONTHLY_SALES = "monthly_sales"
COLLECTION_METADATA = "metadata"

# Database configuration
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "./data/database/analytics.db")

# Prefect configuration
PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")

# API configuration
API_HOST = os.getenv("API_HOST", "localhost")
API_PORT = int(os.getenv("API_PORT", "8000"))

# Buckets
BUCKET_SOURCES = "sources"
BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"
BUCKET_GOLD = "gold"

def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )

def get_mongodb_client() -> MongoClient:
    return MongoClient(
        host=MONGODB_HOST,
        port=MONGODB_PORT,
        username=MONGODB_USERNAME,
        password=MONGODB_PASSWORD
    )

def get_mongodb_database():
    client = get_mongodb_client()
    return client[MONGODB_DATABASE]

def configure_prefect() -> None:
    os.environ["PREFECT_API_URL"] = PREFECT_API_URL

if __name__ == "main":
    client = get_minio_client()
    print(client.list_buckets())

    configure_prefect()
    print(os.getenv("PREFECT_API_URL"))
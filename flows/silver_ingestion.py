from io import BytesIO
from datetime import datetime
from typing import Dict, Tuple

import pandas as pd
from prefect import flow, task

from config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client


@task(name="load_from_bronze", retries=2)
def load_data_from_bronze(object_name: str) -> pd.DataFrame:
    """
    Load CSV data from bronze bucket into pandas DataFrame.

    Args:
        object_name: Name of the object in bronze bucket

    Returns:
        DataFrame with raw data
    """
    client = get_minio_client()
    
    response = client.get_object(BUCKET_BRONZE, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    
    df = pd.read_csv(BytesIO(data))
    print(f"Loaded {len(df)} rows from {object_name}")
    return df


@task(name="quality_check_initial")
def quality_check_initial(df: pd.DataFrame, dataset_name: str) -> Dict:
    """
    Perform initial quality checks on raw data.

    Args:
        df: Input DataFrame
        dataset_name: Name of the dataset for reporting

    Returns:
        Dictionary with quality metrics
    """
    metrics = {
        "dataset": dataset_name,
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "duplicates": df.duplicated().sum(),
        "missing_values": df.isnull().sum().to_dict(),
        "dtypes": df.dtypes.astype(str).to_dict()
    }
    
    print(f"\n=== Quality Check: {dataset_name} ===")
    print(f"Total rows: {metrics['total_rows']}")
    print(f"Duplicates: {metrics['duplicates']}")
    print(f"Missing values per column:")
    for col, missing in metrics['missing_values'].items():
        if missing > 0:
            print(f"  - {col}: {missing} ({missing/len(df)*100:.2f}%)")
    
    return metrics


@task(name="clean_clients_data")
def clean_clients_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Clean and transform clients data.
    
    Transformations:
    - Remove null values in critical columns (client_id, email)
    - Standardize date format (date_inscription)
    - Normalize data types
    - Remove duplicates based on client_id and email
    - Clean and validate email addresses
    - Standardize country names
    
    Args:
        df: Raw clients DataFrame

    Returns:
        Tuple of (cleaned DataFrame, cleaning stats)
    """
    initial_count = len(df)
    df_clean = df.copy()
    
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
    before = len(df_clean)
    df_clean = df_clean.dropna(subset=critical_cols)
    stats["removed_null_critical"] = before - len(df_clean)
    
    # 2. Fill missing values in non-critical columns
    df_clean["name"] = df_clean["name"].fillna("Unknown")
    df_clean["country"] = df_clean["country"].fillna("Unknown")
    
    # 3. Standardize date format and remove invalid dates
    before = len(df_clean)
    df_clean["date_inscription"] = pd.to_datetime(
        df_clean["date_inscription"], 
        errors="coerce"
    )
    df_clean = df_clean.dropna(subset=["date_inscription"])
    stats["removed_invalid_dates"] = before - len(df_clean)
    
    # Remove future dates (aberrant data)
    today = pd.Timestamp.now()
    df_clean = df_clean[df_clean["date_inscription"] <= today]
    
    # 4. Clean and validate email addresses
    before = len(df_clean)
    df_clean["email"] = df_clean["email"].str.strip().str.lower()
    # Basic email validation
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    df_clean = df_clean[df_clean["email"].str.match(email_pattern, na=False)]
    stats["removed_invalid_emails"] = before - len(df_clean)
    
    # 5. Normalize data types
    df_clean["client_id"] = df_clean["client_id"].astype(int)
    df_clean["name"] = df_clean["name"].astype(str).str.strip()
    df_clean["country"] = df_clean["country"].astype(str).str.strip()
    
    # 6. Remove duplicates (keep first occurrence)
    before = len(df_clean)
    # First by client_id
    df_clean = df_clean.drop_duplicates(subset=["client_id"], keep="first")
    # Then by email (in case different IDs have same email)
    df_clean = df_clean.drop_duplicates(subset=["email"], keep="first")
    stats["removed_duplicates"] = before - len(df_clean)
    
    # 7. Sort by client_id
    df_clean = df_clean.sort_values("client_id").reset_index(drop=True)
    
    stats["final_rows"] = len(df_clean)
    stats["data_loss_percentage"] = (initial_count - len(df_clean)) / initial_count * 100
    
    print(f"\n=== Clients Cleaning Stats ===")
    print(f"Initial rows: {stats['initial_rows']}")
    print(f"Removed (null critical columns): {stats['removed_null_critical']}")
    print(f"Removed (invalid dates): {stats['removed_invalid_dates']}")
    print(f"Removed (invalid emails): {stats['removed_invalid_emails']}")
    print(f"Removed (duplicates): {stats['removed_duplicates']}")
    print(f"Final rows: {stats['final_rows']}")
    print(f"Data loss: {stats['data_loss_percentage']:.2f}%")
    
    return df_clean, stats


@task(name="clean_purchases_data")
def clean_purchases_data(df: pd.DataFrame, valid_client_ids: set) -> Tuple[pd.DataFrame, Dict]:
    """
    Clean and transform purchases data.
    
    Transformations:
    - Remove null values in critical columns
    - Standardize date format (date_purchase)
    - Normalize data types
    - Remove duplicates
    - Validate amounts (remove negative or zero values)
    - Validate client_id (must exist in clients)
    - Remove aberrant amounts (statistical outliers)
    
    Args:
        df: Raw purchases DataFrame
        valid_client_ids: Set of valid client IDs from cleaned clients data

    Returns:
        Tuple of (cleaned DataFrame, cleaning stats)
    """
    initial_count = len(df)
    df_clean = df.copy()
    
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
    before = len(df_clean)
    df_clean = df_clean.dropna(subset=critical_cols)
    stats["removed_null_critical"] = before - len(df_clean)
    
    # 2. Fill missing product names
    df_clean["product"] = df_clean["product"].fillna("Unknown Product")
    
    # 3. Standardize date format and remove invalid dates
    before = len(df_clean)
    df_clean["date_purchase"] = pd.to_datetime(
        df_clean["date_purchase"], 
        errors="coerce"
    )
    df_clean = df_clean.dropna(subset=["date_purchase"])
    stats["removed_invalid_dates"] = before - len(df_clean)
    
    # Remove future dates (aberrant data)
    today = pd.Timestamp.now()
    df_clean = df_clean[df_clean["date_purchase"] <= today]
    
    # 4. Normalize data types
    df_clean["purchase_id"] = df_clean["purchase_id"].astype(int)
    df_clean["client_id"] = df_clean["client_id"].astype(int)
    df_clean["amount"] = df_clean["amount"].astype(float)
    df_clean["product"] = df_clean["product"].astype(str).str.strip()
    
    # 5. Remove invalid amounts (negative or zero)
    before = len(df_clean)
    df_clean = df_clean[df_clean["amount"] > 0]
    stats["removed_invalid_amounts"] = before - len(df_clean)
    
    # 6. Remove purchases with invalid client_id (referential integrity)
    before = len(df_clean)
    df_clean = df_clean[df_clean["client_id"].isin(valid_client_ids)]
    stats["removed_invalid_clients"] = before - len(df_clean)
    
    # 7. Remove statistical outliers (amounts > Q3 + 3*IQR)
    before = len(df_clean)
    Q1 = df_clean["amount"].quantile(0.25)
    Q3 = df_clean["amount"].quantile(0.75)
    IQR = Q3 - Q1
    upper_bound = Q3 + 3 * IQR
    lower_bound = Q1 - 3 * IQR
    
    print(f"Amount bounds: [{lower_bound:.2f}, {upper_bound:.2f}]")
    df_clean = df_clean[
        (df_clean["amount"] >= lower_bound) & 
        (df_clean["amount"] <= upper_bound)
    ]
    stats["removed_outliers"] = before - len(df_clean)
    
    # 8. Remove duplicates (keep first occurrence)
    before = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=["purchase_id"], keep="first")
    stats["removed_duplicates"] = before - len(df_clean)
    
    # 9. Sort by purchase_id
    df_clean = df_clean.sort_values("purchase_id").reset_index(drop=True)
    
    stats["final_rows"] = len(df_clean)
    stats["data_loss_percentage"] = (initial_count - len(df_clean)) / initial_count * 100
    
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


@task(name="quality_check_final")
def quality_check_final(df: pd.DataFrame, dataset_name: str) -> Dict:
    """
    Perform final quality checks on cleaned data.

    Args:
        df: Cleaned DataFrame
        dataset_name: Name of the dataset for reporting

    Returns:
        Dictionary with quality metrics
    """
    metrics = {
        "dataset": dataset_name,
        "total_rows": len(df),
        "duplicates": df.duplicated().sum(),
        "missing_values": df.isnull().sum().sum(),
        "dtypes": df.dtypes.astype(str).to_dict()
    }
    
    print(f"\n=== Final Quality Check: {dataset_name} ===")
    print(f"Total rows: {metrics['total_rows']}")
    print(f"Duplicates: {metrics['duplicates']}")
    print(f"Total missing values: {metrics['missing_values']}")
    print(f"Data types: {metrics['dtypes']}")
    
    # Assert quality standards
    assert metrics["duplicates"] == 0, "Duplicates found in cleaned data!"
    
    return metrics


@task(name="save_to_silver", retries=2)
def save_to_silver(df: pd.DataFrame, object_name: str) -> str:
    """
    Save cleaned DataFrame to silver bucket in Parquet format.

    Args:
        df: Cleaned DataFrame
        object_name: Name for the object in silver bucket (without extension)

    Returns:
        Object name in silver bucket
    """
    client = get_minio_client()
    
    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)
    
    # Convert to Parquet (more efficient than CSV)
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_buffer.seek(0)
    
    object_name_parquet = f"{object_name}.parquet"
    
    client.put_object(
        BUCKET_SILVER,
        object_name_parquet,
        parquet_buffer,
        length=len(parquet_buffer.getvalue()),
        content_type="application/octet-stream"
    )
    
    print(f"Saved {len(df)} rows to {BUCKET_SILVER}/{object_name_parquet}")
    return object_name_parquet


@flow(name="Silver Transformation Flow")
def silver_transformation_flow() -> Dict:
    """
    Main flow: Transform bronze data to silver (cleaned and standardized).
    
    Process:
    1. Load data from bronze bucket
    2. Perform initial quality checks
    3. Clean and transform data
    4. Perform final quality checks
    5. Save to silver bucket in Parquet format
    
    Returns:
        Dictionary with transformation results and statistics
    """
    
    # === CLIENTS TRANSFORMATION ===
    print("\n" + "="*50)
    print("CLIENTS TRANSFORMATION")
    print("="*50)
    
    # Load clients data
    df_clients_raw = load_data_from_bronze("clients.csv")
    
    # Initial quality check
    clients_initial_metrics = quality_check_initial(df_clients_raw, "clients")
    
    # Clean clients data
    df_clients_clean, clients_stats = clean_clients_data(df_clients_raw)
    
    # Final quality check
    clients_final_metrics = quality_check_final(df_clients_clean, "clients")
    
    # Save to silver
    clients_silver = save_to_silver(df_clients_clean, "clients")
    
    # Get valid client IDs for purchases validation
    valid_client_ids = set(df_clients_clean["client_id"].unique())
    
    # === PURCHASES TRANSFORMATION ===
    print("\n" + "="*50)
    print("PURCHASES TRANSFORMATION")
    print("="*50)
    
    # Load purchases data
    df_purchases_raw = load_data_from_bronze("purchases.csv")
    
    # Initial quality check
    purchases_initial_metrics = quality_check_initial(df_purchases_raw, "purchases")
    
    # Clean purchases data
    df_purchases_clean, purchases_stats = clean_purchases_data(
        df_purchases_raw, 
        valid_client_ids
    )
    
    # Final quality check
    purchases_final_metrics = quality_check_final(df_purchases_clean, "purchases")
    
    # Save to silver
    purchases_silver = save_to_silver(df_purchases_clean, "purchases")

    print("\n" + "="*50)
    print("TRANSFORMATION SUMMARY")
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
    print(f"\nSilver transformation complete!")
    
    return result


if __name__ == "__main__":
    result = silver_transformation_flow()

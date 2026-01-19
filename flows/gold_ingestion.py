from io import BytesIO
from datetime import datetime
from typing import Dict, Tuple

import pandas as pd
from prefect import flow, task

from config import BUCKET_SILVER, BUCKET_GOLD, get_minio_client


@task(name="load_from_silver", retries=2)
def load_data_from_silver(object_name: str) -> pd.DataFrame:
    """
    Load Parquet data from silver bucket into pandas DataFrame.

    Args:
        object_name: Name of the object in silver bucket

    Returns:
        DataFrame with cleaned data
    """
    client = get_minio_client()
    
    response = client.get_object(BUCKET_SILVER, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    
    df = pd.read_parquet(BytesIO(data))
    print(f"Loaded {len(df)} rows from {object_name}")
    return df


@task(name="create_fact_table")
def create_fact_purchases(
    df_purchases: pd.DataFrame, 
    df_clients: pd.DataFrame
) -> pd.DataFrame:
    """
    Create fact table by joining purchases with client dimensions.

    Args:
        df_purchases: Cleaned purchases DataFrame
        df_clients: Cleaned clients DataFrame

    Returns:
        Fact table with denormalized data
    """
    # Join purchases with clients
    fact_table = df_purchases.merge(
        df_clients,
        on="client_id",
        how="left",
        suffixes=("_purchase", "_client")
    )
    
    # Add temporal dimensions
    fact_table["year"] = fact_table["date_purchase"].dt.year
    fact_table["month"] = fact_table["date_purchase"].dt.month
    fact_table["quarter"] = fact_table["date_purchase"].dt.quarter
    fact_table["week"] = fact_table["date_purchase"].dt.isocalendar().week
    fact_table["day_of_week"] = fact_table["date_purchase"].dt.dayofweek
    fact_table["day_name"] = fact_table["date_purchase"].dt.day_name()
    fact_table["month_name"] = fact_table["date_purchase"].dt.month_name()
    
    # Add year-month for easy grouping
    fact_table["year_month"] = fact_table["date_purchase"].dt.to_period("M").astype(str)
    fact_table["year_week"] = fact_table["date_purchase"].dt.to_period("W").astype(str)
    
    print(f"Created fact table with {len(fact_table)} rows")
    return fact_table


@task(name="create_dimension_clients")
def create_dimension_clients(df_clients: pd.DataFrame) -> pd.DataFrame:
    """
    Create client dimension table with enriched attributes.

    Args:
        df_clients: Cleaned clients DataFrame

    Returns:
        Client dimension table
    """
    dim_clients = df_clients.copy()
    
    # Add temporal attributes
    dim_clients["inscription_year"] = dim_clients["date_inscription"].dt.year
    dim_clients["inscription_month"] = dim_clients["date_inscription"].dt.month
    dim_clients["inscription_quarter"] = dim_clients["date_inscription"].dt.quarter
    
    # Calculate client age (days since inscription)
    today = pd.Timestamp.now()
    dim_clients["client_age_days"] = (today - dim_clients["date_inscription"]).dt.days
    
    # Client segment based on age
    def client_segment(days):
        if days < 90:
            return "New"
        elif days < 365:
            return "Active"
        else:
            return "Loyal"
    
    dim_clients["client_segment"] = dim_clients["client_age_days"].apply(client_segment)
    
    print(f"Created client dimension with {len(dim_clients)} rows")
    return dim_clients


@task(name="create_dimension_products")
def create_dimension_products(df_purchases: pd.DataFrame) -> pd.DataFrame:
    """
    Create product dimension table.

    Args:
        df_purchases: Cleaned purchases DataFrame

    Returns:
        Product dimension table
    """
    dim_products = df_purchases[["product"]].drop_duplicates().reset_index(drop=True)
    dim_products["product_id"] = range(1, len(dim_products) + 1)
    
    print(f"Created product dimension with {len(dim_products)} rows")
    return dim_products


@task(name="aggregate_kpi_global")
def aggregate_kpi_global(fact_table: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate global KPIs.

    Metrics:
    - Total revenue
    - Total transactions
    - Average transaction value
    - Unique clients
    - Unique products

    Args:
        fact_table: Fact table with all data

    Returns:
        DataFrame with global KPIs
    """
    kpis = pd.DataFrame([{
        "metric": "Total Revenue (€)",
        "value": fact_table["amount"].sum(),
        "type": "financial"
    }, {
        "metric": "Total Transactions",
        "value": fact_table["purchase_id"].nunique(),
        "type": "volume"
    }, {
        "metric": "Average Transaction Value (€)",
        "value": fact_table["amount"].mean(),
        "type": "financial"
    }, {
        "metric": "Median Transaction Value (€)",
        "value": fact_table["amount"].median(),
        "type": "financial"
    }, {
        "metric": "Total Unique Clients",
        "value": fact_table["client_id"].nunique(),
        "type": "clients"
    }, {
        "metric": "Total Unique Products",
        "value": fact_table["product"].nunique(),
        "type": "products"
    }, {
        "metric": "Average Transactions per Client",
        "value": len(fact_table) / fact_table["client_id"].nunique(),
        "type": "behavior"
    }])
    
    print(f"\n=== Global KPIs ===")
    for _, row in kpis.iterrows():
        print(f"{row['metric']}: {row['value']:,.2f}")
    
    return kpis


@task(name="aggregate_by_country")
def aggregate_by_country(fact_table: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate revenue and volume by country.

    Args:
        fact_table: Fact table with all data

    Returns:
        DataFrame with country-level aggregations
    """
    country_agg = fact_table.groupby("country").agg({
        "amount": ["sum", "mean", "count"],
        "client_id": "nunique",
        "product": "nunique"
    }).reset_index()
    
    # Flatten column names
    country_agg.columns = [
        "country", 
        "total_revenue", 
        "avg_transaction_value", 
        "total_transactions",
        "unique_clients",
        "unique_products"
    ]
    
    # Calculate revenue share
    country_agg["revenue_share_pct"] = (
        country_agg["total_revenue"] / country_agg["total_revenue"].sum() * 100
    )
    
    # Sort by revenue
    country_agg = country_agg.sort_values("total_revenue", ascending=False)
    
    print(f"\n=== Top 5 Countries by Revenue ===")
    print(country_agg.head()[["country", "total_revenue", "revenue_share_pct"]])
    
    return country_agg


@task(name="aggregate_by_product")
def aggregate_by_product(fact_table: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate revenue and volume by product.

    Args:
        fact_table: Fact table with all data

    Returns:
        DataFrame with product-level aggregations
    """
    product_agg = fact_table.groupby("product").agg({
        "amount": ["sum", "mean", "count"],
        "client_id": "nunique"
    }).reset_index()
    
    # Flatten column names
    product_agg.columns = [
        "product",
        "total_revenue",
        "avg_price",
        "total_sales",
        "unique_buyers"
    ]
    
    # Calculate revenue share
    product_agg["revenue_share_pct"] = (
        product_agg["total_revenue"] / product_agg["total_revenue"].sum() * 100
    )
    
    # Sort by revenue
    product_agg = product_agg.sort_values("total_revenue", ascending=False)
    
    print(f"\n=== Product Performance ===")
    print(product_agg[["product", "total_revenue", "total_sales", "revenue_share_pct"]])
    
    return product_agg


@task(name="aggregate_by_time_daily")
def aggregate_by_time_daily(fact_table: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate daily aggregations.

    Args:
        fact_table: Fact table with all data

    Returns:
        DataFrame with daily aggregations
    """
    daily_agg = fact_table.groupby(
        fact_table["date_purchase"].dt.date
    ).agg({
        "amount": ["sum", "mean", "count"],
        "client_id": "nunique"
    }).reset_index()
    
    # Flatten column names
    daily_agg.columns = [
        "date",
        "daily_revenue",
        "avg_transaction_value",
        "daily_transactions",
        "daily_unique_clients"
    ]
    
    # Calculate moving averages (7-day window)
    daily_agg["revenue_ma7"] = daily_agg["daily_revenue"].rolling(window=7, min_periods=1).mean()
    daily_agg["transactions_ma7"] = daily_agg["daily_transactions"].rolling(window=7, min_periods=1).mean()
    
    print(f"Created daily aggregations with {len(daily_agg)} days")
    return daily_agg


@task(name="aggregate_by_time_weekly")
def aggregate_by_time_weekly(fact_table: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate weekly aggregations.

    Args:
        fact_table: Fact table with all data

    Returns:
        DataFrame with weekly aggregations
    """
    weekly_agg = fact_table.groupby("year_week").agg({
        "amount": ["sum", "mean", "count"],
        "client_id": "nunique",
        "product": "nunique"
    }).reset_index()
    
    # Flatten column names
    weekly_agg.columns = [
        "year_week",
        "weekly_revenue",
        "avg_transaction_value",
        "weekly_transactions",
        "weekly_unique_clients",
        "weekly_unique_products"
    ]
    
    print(f"Created weekly aggregations with {len(weekly_agg)} weeks")
    return weekly_agg


@task(name="aggregate_by_time_monthly")
def aggregate_by_time_monthly(fact_table: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate monthly aggregations with growth rates.

    Args:
        fact_table: Fact table with all data

    Returns:
        DataFrame with monthly aggregations and growth metrics
    """
    monthly_agg = fact_table.groupby("year_month").agg({
        "amount": ["sum", "mean", "count"],
        "client_id": "nunique",
        "product": "nunique"
    }).reset_index()
    
    # Flatten column names
    monthly_agg.columns = [
        "year_month",
        "monthly_revenue",
        "avg_transaction_value",
        "monthly_transactions",
        "monthly_unique_clients",
        "monthly_unique_products"
    ]
    
    # Calculate month-over-month growth rates
    monthly_agg["revenue_mom_growth_pct"] = (
        monthly_agg["monthly_revenue"].pct_change() * 100
    )
    monthly_agg["transactions_mom_growth_pct"] = (
        monthly_agg["monthly_transactions"].pct_change() * 100
    )
    monthly_agg["clients_mom_growth_pct"] = (
        monthly_agg["monthly_unique_clients"].pct_change() * 100
    )
    
    # Calculate cumulative revenue
    monthly_agg["cumulative_revenue"] = monthly_agg["monthly_revenue"].cumsum()
    
    print(f"\n=== Monthly Revenue Trend (Last 6 months) ===")
    print(monthly_agg[["year_month", "monthly_revenue", "revenue_mom_growth_pct"]].tail(6))
    
    return monthly_agg


@task(name="aggregate_by_time_quarterly")
def aggregate_by_time_quarterly(fact_table: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate quarterly aggregations.

    Args:
        fact_table: Fact table with all data

    Returns:
        DataFrame with quarterly aggregations
    """
    quarterly_agg = fact_table.groupby(["year", "quarter"]).agg({
        "amount": ["sum", "mean", "count"],
        "client_id": "nunique"
    }).reset_index()
    
    # Flatten column names
    quarterly_agg.columns = [
        "year",
        "quarter",
        "quarterly_revenue",
        "avg_transaction_value",
        "quarterly_transactions",
        "quarterly_unique_clients"
    ]
    
    # Create quarter label
    quarterly_agg["year_quarter"] = (
        quarterly_agg["year"].astype(str) + "-Q" + quarterly_agg["quarter"].astype(str)
    )
    
    # Calculate quarter-over-quarter growth
    quarterly_agg["revenue_qoq_growth_pct"] = (
        quarterly_agg["quarterly_revenue"].pct_change() * 100
    )
    
    print(f"Created quarterly aggregations with {len(quarterly_agg)} quarters")
    return quarterly_agg


@task(name="aggregate_client_behavior")
def aggregate_client_behavior(fact_table: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate client behavior metrics (RFM-like analysis).

    Args:
        fact_table: Fact table with all data

    Returns:
        DataFrame with client-level behavior metrics
    """
    today = fact_table["date_purchase"].max()
    
    client_behavior = fact_table.groupby("client_id").agg({
        "purchase_id": "count",  # Frequency
        "amount": ["sum", "mean", "min", "max", "std"],
        "date_purchase": ["min", "max"],
        "product": "nunique",
        "country": "first",
        "name": "first"
    }).reset_index()
    
    # Flatten column names
    client_behavior.columns = [
        "client_id",
        "total_purchases",
        "total_spent",
        "avg_purchase_value",
        "min_purchase_value",
        "max_purchase_value",
        "std_purchase_value",
        "first_purchase_date",
        "last_purchase_date",
        "unique_products_bought",
        "country",
        "name"
    ]
    
    # Calculate recency (days since last purchase)
    client_behavior["recency_days"] = (
        today - client_behavior["last_purchase_date"]
    ).dt.days
    
    # Calculate customer lifetime (days between first and last purchase)
    client_behavior["customer_lifetime_days"] = (
        client_behavior["last_purchase_date"] - client_behavior["first_purchase_date"]
    ).dt.days
    
    # Calculate purchase frequency (purchases per day)
    client_behavior["purchase_frequency"] = (
        client_behavior["total_purchases"] / 
        (client_behavior["customer_lifetime_days"] + 1)  # +1 to avoid division by zero
    )
    
    # Client segmentation (RFM-inspired)
    def segment_client(row):
        if row["recency_days"] <= 30 and row["total_purchases"] >= 5:
            return "VIP"
        elif row["recency_days"] <= 90 and row["total_purchases"] >= 3:
            return "Active"
        elif row["recency_days"] <= 180:
            return "At Risk"
        else:
            return "Inactive"
    
    client_behavior["client_segment"] = client_behavior.apply(segment_client, axis=1)
    
    # Sort by total spent
    client_behavior = client_behavior.sort_values("total_spent", ascending=False)
    
    print(f"\n=== Client Segmentation ===")
    print(client_behavior["client_segment"].value_counts())
    
    print(f"\n=== Top 5 Clients by Revenue ===")
    print(client_behavior[["name", "total_spent", "total_purchases", "client_segment"]].head())
    
    return client_behavior


@task(name="aggregate_statistical_distributions")
def aggregate_statistical_distributions(fact_table: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate statistical distributions for key metrics.

    Args:
        fact_table: Fact table with all data

    Returns:
        DataFrame with statistical metrics
    """
    stats = pd.DataFrame([
        {
            "metric": "Transaction Amount",
            "mean": fact_table["amount"].mean(),
            "median": fact_table["amount"].median(),
            "std": fact_table["amount"].std(),
            "min": fact_table["amount"].min(),
            "max": fact_table["amount"].max(),
            "q25": fact_table["amount"].quantile(0.25),
            "q75": fact_table["amount"].quantile(0.75),
            "skewness": fact_table["amount"].skew(),
            "kurtosis": fact_table["amount"].kurtosis()
        }
    ])
    
    print(f"\n=== Statistical Distribution: Transaction Amount ===")
    print(stats.T)
    
    return stats


@task(name="aggregate_country_product_matrix")
def aggregate_country_product_matrix(fact_table: pd.DataFrame) -> pd.DataFrame:
    """
    Create a pivot table of revenue by country and product.

    Args:
        fact_table: Fact table with all data

    Returns:
        Pivot table DataFrame
    """
    matrix = fact_table.pivot_table(
        index="country",
        columns="product",
        values="amount",
        aggfunc="sum",
        fill_value=0
    ).reset_index()
    
    print(f"Created country-product matrix: {matrix.shape}")
    return matrix


@task(name="save_to_gold", retries=2)
def save_to_gold(df: pd.DataFrame, object_name: str) -> str:
    """
    Save aggregated DataFrame to gold bucket in Parquet format.

    Args:
        df: Aggregated DataFrame
        object_name: Name for the object in gold bucket (without extension)

    Returns:
        Object name in gold bucket
    """
    client = get_minio_client()
    
    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)
    
    # Convert to Parquet
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_buffer.seek(0)
    
    object_name_parquet = f"{object_name}.parquet"
    
    client.put_object(
        BUCKET_GOLD,
        object_name_parquet,
        parquet_buffer,
        length=len(parquet_buffer.getvalue()),
        content_type="application/octet-stream"
    )
    
    print(f"Saved {len(df)} rows to {BUCKET_GOLD}/{object_name_parquet}")
    return object_name_parquet


@flow(name="Gold Aggregation Flow")
def gold_aggregation_flow() -> Dict:
    """
    Main flow: Create gold layer with business aggregations and KPIs.
    
    Process:
    1. Load cleaned data from silver bucket
    2. Create fact and dimension tables
    3. Calculate global KPIs
    4. Aggregate by dimensions (country, product, time)
    5. Calculate growth rates and trends
    6. Analyze client behavior
    7. Generate statistical distributions
    8. Save all aggregations to gold bucket
    
    Returns:
        Dictionary with all gold objects created
    """
    
    print("\n" + "="*60)
    print("GOLD LAYER - BUSINESS AGGREGATIONS & KPIs")
    print("="*60)
    
    # === LOAD DATA FROM SILVER ===
    print("\n[1/4] Loading data from silver layer...")
    df_purchases = load_data_from_silver("purchases.parquet")
    df_clients = load_data_from_silver("clients.parquet")
    
    # === CREATE FACT & DIMENSION TABLES ===
    print("\n[2/4] Creating fact and dimension tables...")
    fact_table = create_fact_purchases(df_purchases, df_clients)
    dim_clients = create_dimension_clients(df_clients)
    dim_products = create_dimension_products(df_purchases)
    
    # Save fact and dimension tables
    fact_table_gold = save_to_gold(fact_table, "fact_purchases")
    dim_clients_gold = save_to_gold(dim_clients, "dim_clients")
    dim_products_gold = save_to_gold(dim_products, "dim_products")
    
    # === CALCULATE KPIs & AGGREGATIONS ===
    print("\n[3/4] Calculating KPIs and aggregations...")
    
    # Global KPIs
    kpi_global = aggregate_kpi_global(fact_table)
    kpi_global_gold = save_to_gold(kpi_global, "kpi_global")
    
    # Dimensional aggregations
    agg_country = aggregate_by_country(fact_table)
    agg_country_gold = save_to_gold(agg_country, "agg_by_country")
    
    agg_product = aggregate_by_product(fact_table)
    agg_product_gold = save_to_gold(agg_product, "agg_by_product")
    
    # Temporal aggregations
    agg_daily = aggregate_by_time_daily(fact_table)
    agg_daily_gold = save_to_gold(agg_daily, "agg_by_day")
    
    agg_weekly = aggregate_by_time_weekly(fact_table)
    agg_weekly_gold = save_to_gold(agg_weekly, "agg_by_week")
    
    agg_monthly = aggregate_by_time_monthly(fact_table)
    agg_monthly_gold = save_to_gold(agg_monthly, "agg_by_month")
    
    agg_quarterly = aggregate_by_time_quarterly(fact_table)
    agg_quarterly_gold = save_to_gold(agg_quarterly, "agg_by_quarter")
    
    # Client behavior analysis
    client_behavior = aggregate_client_behavior(fact_table)
    client_behavior_gold = save_to_gold(client_behavior, "client_behavior_rfm")
    
    # Statistical distributions
    stats_distributions = aggregate_statistical_distributions(fact_table)
    stats_gold = save_to_gold(stats_distributions, "statistical_distributions")
    
    # Country-Product matrix
    country_product_matrix = aggregate_country_product_matrix(fact_table)
    matrix_gold = save_to_gold(country_product_matrix, "matrix_country_product")
    
    # === SUMMARY ===
    print("\n[4/4] Gold layer complete!")
    print("="*60)
    
    result = {
        "fact_dimension_tables": {
            "fact_purchases": fact_table_gold,
            "dim_clients": dim_clients_gold,
            "dim_products": dim_products_gold
        },
        "kpis": {
            "global": kpi_global_gold
        },
        "aggregations": {
            "by_country": agg_country_gold,
            "by_product": agg_product_gold,
            "by_day": agg_daily_gold,
            "by_week": agg_weekly_gold,
            "by_month": agg_monthly_gold,
            "by_quarter": agg_quarterly_gold,
            "client_behavior": client_behavior_gold,
            "country_product_matrix": matrix_gold
        },
        "statistics": {
            "distributions": stats_gold
        },
        "timestamp": datetime.now().isoformat()
    }
    
    print(f"\n✓ Created {len([v for d in result.values() for v in (d.values() if isinstance(d, dict) else [d])])} gold objects")
    print(f"✓ Fact table: {len(fact_table):,} rows")
    print(f"✓ Ready for analytics and visualization!")
    
    return result


if __name__ == "__main__":
    result = gold_aggregation_flow()
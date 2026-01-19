import pandas as pd
from io import BytesIO
from prefect import flow, task

from config import BUCKET_SILVER, BUCKET_GOLD, get_minio_client


# --------------------------------------------------
# UTILITAIRES MINIO
# --------------------------------------------------

def read_csv_from_minio(bucket: str, object_name: str) -> pd.DataFrame:
    client = get_minio_client()
    response = client.get_object(bucket, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    return pd.read_csv(BytesIO(data))


def write_csv_to_minio(bucket: str, object_name: str, df: pd.DataFrame):
    client = get_minio_client()

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    buf = BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)

    client.put_object(bucket, object_name, buf, length=buf.getbuffer().nbytes)
    print(f"Saved {bucket}/{object_name} ({len(df)} rows)")


# --------------------------------------------------
# GOLD BUILDERS
# --------------------------------------------------

@task(name="build_gold_clients_kpis")
def build_gold_clients_kpis() -> str:
    clients = read_csv_from_minio(BUCKET_SILVER, "clients.csv")
    achats = read_csv_from_minio(BUCKET_SILVER, "achats.csv")

    # Types / dates (au cas où)
    clients["client_id"] = clients["client_id"].astype(int)
    achats["client_id"] = achats["client_id"].astype(int)
    achats["montant_achat"] = achats["montant_achat"].astype(float)
    achats["date_achat"] = pd.to_datetime(achats["date_achat"], errors="coerce")

    agg = (
        achats.groupby("client_id", as_index=False)
        .agg(
            nb_achats=("id_achat", "count"),
            total_achats=("montant_achat", "sum"),
            panier_moyen=("montant_achat", "mean"),
            premier_achat=("date_achat", "min"),
            dernier_achat=("date_achat", "max"),
        )
    )

    gold = clients.merge(agg, on="client_id", how="left")

    # Clients sans achats => KPI à 0 / NA cohérents
    gold["nb_achats"] = gold["nb_achats"].fillna(0).astype(int)
    gold["total_achats"] = gold["total_achats"].fillna(0.0)
    gold["panier_moyen"] = gold["panier_moyen"].fillna(0.0)

    # QA simple
    assert gold["client_id"].is_unique, "GOLD clients_kpis: client_id non unique"

    write_csv_to_minio(BUCKET_GOLD, "gold_clients_kpis.csv", gold)
    return "gold_clients_kpis.csv"


@task(name="build_gold_ca_mensuel")
def build_gold_ca_mensuel() -> str:
    achats = read_csv_from_minio(BUCKET_SILVER, "achats.csv")
    achats["date_achat"] = pd.to_datetime(achats["date_achat"], errors="coerce")
    achats["montant_achat"] = pd.to_numeric(achats["montant_achat"], errors="coerce")

    df = achats.dropna(subset=["date_achat", "montant_achat"]).copy()
    df["mois"] = df["date_achat"].dt.to_period("M").astype(str)

    ca_mensuel = (
        df.groupby("mois", as_index=False)
        .agg(
            chiffre_affaires=("montant_achat", "sum"),
            nb_achats=("id_achat", "count"),
            panier_moyen=("montant_achat", "mean"),
        )
        .sort_values("mois")
        .reset_index(drop=True)
    )

    write_csv_to_minio(BUCKET_GOLD, "gold_ca_mensuel.csv", ca_mensuel)
    return "gold_ca_mensuel.csv"


@task(name="build_gold_top_produits")
def build_gold_top_produits(top_n: int = 10) -> str:
    achats = read_csv_from_minio(BUCKET_SILVER, "achats.csv")
    achats["montant_achat"] = pd.to_numeric(achats["montant_achat"], errors="coerce")

    df = achats.dropna(subset=["produit", "montant_achat"]).copy()
    df["produit"] = df["produit"].astype(str).str.strip().str.title()

    top = (
        df.groupby("produit", as_index=False)
        .agg(
            nb_ventes=("id_achat", "count"),
            montant_total=("montant_achat", "sum"),
            montant_moyen=("montant_achat", "mean"),
        )
        .sort_values(["montant_total", "nb_ventes"], ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )

    write_csv_to_minio(BUCKET_GOLD, "gold_top_produits.csv", top)
    return "gold_top_produits.csv"


@task(name="gold_quality_checks")
def gold_quality_checks() -> bool:
    kpis = read_csv_from_minio(BUCKET_GOLD, "gold_clients_kpis.csv")
    ca = read_csv_from_minio(BUCKET_GOLD, "gold_ca_mensuel.csv")
    top = read_csv_from_minio(BUCKET_GOLD, "gold_top_produits.csv")

    # QA basique
    assert kpis["client_id"].is_unique, "QA GOLD: client_id non unique dans gold_clients_kpis"
    assert (ca["chiffre_affaires"] >= 0).all(), "QA GOLD: CA négatif détecté"
    assert (top["montant_total"] >= 0).all(), "QA GOLD: montant_total négatif détecté"

    print("QA GOLD OK")
    return True


# --------------------------------------------------
# FLOW GOLD
# --------------------------------------------------

@flow(name="Gold Aggregation Flow")
def gold_aggregation_flow(top_n_produits: int = 10) -> dict:
    clients_kpis = build_gold_clients_kpis()
    ca_mensuel = build_gold_ca_mensuel()
    top_produits = build_gold_top_produits(top_n=top_n_produits)

    gold_quality_checks()

    return {
        "gold_clients_kpis": clients_kpis,
        "gold_ca_mensuel": ca_mensuel,
        "gold_top_produits": top_produits,
    }


if __name__ == "__main__":
    result = gold_aggregation_flow(top_n_produits=10)
    print("Gold aggregation complete:", result)
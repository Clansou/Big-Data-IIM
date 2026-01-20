import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from io import BytesIO
import sys
from pathlib import Path

# Ajouter le dossier parent au PYTHONPATH
sys.path.append(str(Path(__file__).parent.parent))
from utils.config import BUCKET_GOLD, get_minio_client


@st.cache_data
def load_gold_data(object_name: str) -> pd.DataFrame:
    """Load data from gold bucket."""
    client = get_minio_client()
    response = client.get_object(BUCKET_GOLD, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    return pd.read_parquet(BytesIO(data))


# Configuration de la page
st.set_page_config(
    page_title="Analytics Dashboard",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Business Analytics Dashboard")
st.markdown("---")

# Sidebar pour la navigation
page = st.sidebar.selectbox(
    "Choisir une vue",
    [
        "🌍 Analyse par Pays", 
        "📅 Tendances Temporelles",
        "👥 Comportement Clients",
    ]
)

# === ANALYSE PAR PAYS ===
if page == "🌍 Analyse par Pays":
    st.header("Analyse des Clients")
    
    client_summary = load_gold_data("gold_client_summary.parquet")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Répartition par pays
        country_stats = client_summary.groupby("country").agg({
            "total_achats": "sum",
            "client_id": "count"
        }).reset_index()
        country_stats.columns = ["country", "total_ca", "nb_clients"]
        country_stats = country_stats.sort_values("total_ca", ascending=False)
        
        fig = px.bar(
            country_stats.head(10),
            x="country",
            y="total_ca",
            title="🏆 Top 10 Pays par CA",
            labels={"country": "Pays", "total_ca": "CA (€)"},
            color="total_ca",
            color_continuous_scale="Blues"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Top clients
        top_clients = client_summary.nlargest(10, "total_achats")
        fig = px.bar(
            top_clients,
            x="name",
            y="total_achats",
            title="🔥 Top 10 Clients par CA",
            labels={"name": "Client", "total_achats": "CA (€)"},
            color="total_achats",
            color_continuous_scale="Reds"
        )
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Tableau détaillé
    st.subheader("📋 Détails des Clients")
    
    # Filtres
    col1, col2 = st.columns(2)
    with col1:
        min_ca = st.number_input("CA minimum", min_value=0, value=0)
    with col2:
        selected_country = st.selectbox("Pays", ["Tous"] + list(client_summary["country"].unique()))
    
    # Filtrer les données
    filtered_data = client_summary[client_summary["total_achats"] >= min_ca]
    if selected_country != "Tous":
        filtered_data = filtered_data[filtered_data["country"] == selected_country]
    
    st.dataframe(
        filtered_data[["name", "email", "country", "total_achats", "panier_moyen", "nb_achats"]].style.format({
            "total_achats": "{:,.2f} €",
            "panier_moyen": "{:,.2f} €"
        }),
        use_container_width=True,
        height=400
    )

# === TENDANCES TEMPORELLES ===
elif page == "📅 Tendances Temporelles":
    st.header("Évolution Temporelle des Ventes")
    
    monthly_sales = load_gold_data("gold_monthly_sales.parquet")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Évolution du CA mensuel
        fig = px.line(
            monthly_sales,
            x="mois",
            y="chiffre_affaires",
            title="📈 Évolution du Chiffre d'Affaires",
            labels={"mois": "Mois", "chiffre_affaires": "CA (€)"},
            markers=True
        )
        fig.update_traces(line_color="#2E86AB", line_width=3, marker_size=8)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Évolution du nombre d'achats
        fig = px.line(
            monthly_sales,
            x="mois",
            y="nb_achats",
            title="📊 Évolution du Nombre d'Achats",
            labels={"mois": "Mois", "nb_achats": "Nombre d'Achats"},
            markers=True
        )
        fig.update_traces(line_color="#A23B72", line_width=3, marker_size=8)
        st.plotly_chart(fig, use_container_width=True)
    
    # Panier moyen
    fig = px.bar(
        monthly_sales,
        x="mois",
        y="panier_moyen",
        title="🛒 Évolution du Panier Moyen",
        labels={"mois": "Mois", "panier_moyen": "Panier Moyen (€)"},
        color="panier_moyen",
        color_continuous_scale="Blues"
    )
    st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Tableau détaillé
    st.subheader("📋 Détails Mensuels")
    st.dataframe(
        monthly_sales.style.format({
            "chiffre_affaires": "{:,.2f} €",
            "panier_moyen": "{:,.2f} €",
            "nb_achats": "{:,.0f}"
        }),
        use_container_width=True,
        height=300
    )

# === COMPORTEMENT CLIENTS ===
elif page == "👥 Comportement Clients":
    st.header("Analyse des Clients")
    
    client_summary = load_gold_data("gold_client_summary.parquet")
    
    # Statistiques globales
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_clients = len(client_summary)
        st.metric("Total Clients", f"{total_clients:,}")
    
    with col2:
        clients_actifs = len(client_summary[client_summary["nb_achats"] > 0])
        st.metric("Clients Actifs", f"{clients_actifs:,}")
    
    with col3:
        avg_spent = client_summary["total_achats"].mean()
        st.metric("Dépense Moyenne", f"{avg_spent:,.2f} €")
    
    with col4:
        avg_orders = client_summary["nb_achats"].mean()
        st.metric("Commandes Moyennes", f"{avg_orders:,.1f}")
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Distribution des dépenses
        fig = px.histogram(
            client_summary,
            x="total_achats",
            nbins=30,
            title="📊 Distribution des Dépenses par Client",
            labels={"total_achats": "Dépenses Totales (€)", "count": "Nombre de Clients"}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Répartition par pays
        country_counts = client_summary["country"].value_counts().reset_index()
        country_counts.columns = ["country", "count"]
        
        fig = px.pie(
            country_counts,
            values="count",
            names="country",
            title="🌍 Répartition des Clients par Pays"
        )
        st.plotly_chart(fig, use_container_width=True)


# Footer
st.sidebar.markdown("---")
st.sidebar.info("""
    **Sources de données:**
    - 3 fichiers Parquet dans MinIO (bucket: gold)
    - Pipeline: Bronze → Silver → Gold
    - Tables: client_summary, product_stats, monthly_sales
""")

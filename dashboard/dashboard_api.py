"""
Dashboard Streamlit qui interroge l'API FastAPI
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
from datetime import datetime
import os

# Configuration de l'API
API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(
    page_title="Analytics Dashboard (API)",
    page_icon="📊",
    layout="wide"
)


def api_get(endpoint: str, params: dict = None) -> dict:
    """Appeler l'API et retourner les données JSON"""
    try:
        response = requests.get(f"{API_URL}{endpoint}", params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Erreur API: {e}")
        return None


def format_currency(value: float) -> str:
    """Formater un nombre en devise"""
    return f"{value:,.2f} EUR"


# ============================================
# SIDEBAR - Informations de refresh
# ============================================
with st.sidebar:
    st.title("Analytics Dashboard")
    st.markdown("---")

    # Informations de refresh
    refresh_info = api_get("/metadata/refresh")
    if refresh_info:
        st.subheader("Dernier Refresh")
        last_refresh = refresh_info.get("last_refresh", "N/A")
        if isinstance(last_refresh, str) and last_refresh != "N/A":
            try:
                dt = datetime.fromisoformat(last_refresh.replace("Z", "+00:00"))
                st.write(f"Date: {dt.strftime('%Y-%m-%d %H:%M:%S')}")
            except:
                st.write(f"Date: {last_refresh}")

        refresh_time = refresh_info.get("total_refresh_time_seconds", 0)
        st.metric("Temps de refresh", f"{refresh_time:.2f}s")

        total_records = refresh_info.get("total_records_loaded", 0)
        st.metric("Records chargés", f"{total_records:,}")

    st.markdown("---")

    # Health check
    health = api_get("/health")
    if health and health.get("status") == "healthy":
        st.success("API: Connectée")
        st.success("MongoDB: Connecté")
    else:
        st.error("API: Déconnectée")

# ============================================
# MAIN CONTENT
# ============================================
st.title("Analytics Dashboard via API")

# Tabs pour les différentes pages
tab1, tab2, tab3, tab4 = st.tabs([
    "Vue d'ensemble",
    "Analyse par Pays",
    "Tendances Temporelles",
    "Produits"
])

# ============================================
# TAB 1: Vue d'ensemble
# ============================================
with tab1:
    st.header("Vue d'ensemble")

    # KPIs globaux
    col1, col2, col3, col4 = st.columns(4)

    # Résumé des ventes
    sales_summary = api_get("/sales/summary")
    if sales_summary:
        with col1:
            st.metric(
                "Chiffre d'affaires total",
                format_currency(sales_summary.get("total_ca", 0))
            )
        with col2:
            st.metric(
                "Nombre de transactions",
                f"{sales_summary.get('total_achats', 0):,}"
            )
        with col3:
            st.metric(
                "Panier moyen",
                format_currency(sales_summary.get("panier_moyen_global", 0))
            )
        with col4:
            st.metric(
                "Mois analysés",
                sales_summary.get("nb_mois", 0)
            )

    # Clients et Produits
    clients_data = api_get("/clients", {"limit": 1})
    products_data = api_get("/products")

    col1, col2 = st.columns(2)
    with col1:
        # Top clients
        top_clients = api_get("/clients/top/10")
        if top_clients and top_clients.get("data"):
            st.subheader("Top 10 Clients")
            df_top = pd.DataFrame(top_clients["data"])
            fig = px.bar(
                df_top,
                x="name",
                y="total_achats",
                color="country",
                title="Top 10 Clients par CA"
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Produits
        if products_data and products_data.get("data"):
            st.subheader("Performance des Produits")
            df_products = pd.DataFrame(products_data["data"])
            fig = px.bar(
                df_products,
                x="product",
                y="chiffre_affaires",
                title="CA par Produit"
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

# ============================================
# TAB 2: Analyse par Pays
# ============================================
with tab2:
    st.header("Analyse par Pays")

    # Statistiques par pays
    stats_by_country = api_get("/clients/stats/by-country")
    if stats_by_country and stats_by_country.get("data"):
        df_country = pd.DataFrame(stats_by_country["data"])

        col1, col2 = st.columns(2)

        with col1:
            # CA par pays
            fig = px.bar(
                df_country,
                x="country",
                y="total_ca",
                title="Chiffre d'affaires par Pays",
                color="total_ca",
                color_continuous_scale="Blues"
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Nombre de clients par pays
            fig = px.pie(
                df_country,
                values="total_clients",
                names="country",
                title="Répartition des Clients par Pays"
            )
            st.plotly_chart(fig, use_container_width=True)

        # Tableau détaillé
        st.subheader("Détails par Pays")
        df_display = df_country.copy()
        df_display.columns = ["Pays", "Clients", "CA Total", "CA Moyen", "Panier Moyen"]
        st.dataframe(df_display, use_container_width=True)

    # Filtrer les clients par pays
    st.markdown("---")
    st.subheader("Recherche de Clients")

    col1, col2, col3 = st.columns(3)
    with col1:
        countries = ["Tous"] + [c["country"] for c in stats_by_country.get("data", [])] if stats_by_country else ["Tous"]
        selected_country = st.selectbox("Pays", countries)
    with col2:
        min_ca = st.number_input("CA Minimum", min_value=0.0, value=0.0, step=1000.0)
    with col3:
        limit = st.slider("Nombre de résultats", 10, 500, 100)

    # Requête API avec filtres
    params = {"limit": limit}
    if selected_country != "Tous":
        params["country"] = selected_country
    if min_ca > 0:
        params["min_total"] = min_ca

    clients = api_get("/clients", params)
    if clients and clients.get("data"):
        df_clients = pd.DataFrame(clients["data"])
        if not df_clients.empty:
            cols_to_show = ["name", "email", "country", "total_achats", "panier_moyen", "nb_achats"]
            df_show = df_clients[[c for c in cols_to_show if c in df_clients.columns]]
            st.dataframe(df_show, use_container_width=True)
            st.caption(f"{len(df_clients)} clients affichés")

# ============================================
# TAB 3: Tendances Temporelles
# ============================================
with tab3:
    st.header("Tendances Temporelles")

    monthly_sales = api_get("/sales/monthly")
    if monthly_sales and monthly_sales.get("data"):
        df_monthly = pd.DataFrame(monthly_sales["data"])

        # Evolution du CA
        fig = px.line(
            df_monthly,
            x="mois",
            y="chiffre_affaires",
            title="Evolution du Chiffre d'Affaires Mensuel",
            markers=True
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

        col1, col2 = st.columns(2)

        with col1:
            # Nombre de transactions
            fig = px.bar(
                df_monthly,
                x="mois",
                y="nb_achats",
                title="Nombre de Transactions par Mois",
                color="nb_achats",
                color_continuous_scale="Greens"
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Panier moyen
            fig = px.line(
                df_monthly,
                x="mois",
                y="panier_moyen",
                title="Evolution du Panier Moyen",
                markers=True
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        # Tableau des données mensuelles
        st.subheader("Données Mensuelles")
        df_display = df_monthly[["mois", "chiffre_affaires", "panier_moyen", "nb_achats"]].copy()
        df_display.columns = ["Mois", "CA", "Panier Moyen", "Transactions"]
        st.dataframe(df_display, use_container_width=True)

# ============================================
# TAB 4: Produits
# ============================================
with tab4:
    st.header("Analyse des Produits")

    products = api_get("/products")
    if products and products.get("data"):
        df_products = pd.DataFrame(products["data"])

        # KPIs produits
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Nombre de produits", len(df_products))
        with col2:
            total_ca = df_products["chiffre_affaires"].sum()
            st.metric("CA Total Produits", format_currency(total_ca))
        with col3:
            total_ventes = df_products["nb_ventes"].sum()
            st.metric("Total Ventes", f"{total_ventes:,}")

        col1, col2 = st.columns(2)

        with col1:
            # Répartition CA par produit
            fig = px.pie(
                df_products,
                values="chiffre_affaires",
                names="product",
                title="Répartition du CA par Produit"
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Prix moyen par produit
            fig = px.bar(
                df_products.sort_values("prix_moyen", ascending=False),
                x="product",
                y="prix_moyen",
                title="Prix Moyen par Produit",
                color="prix_moyen",
                color_continuous_scale="Oranges"
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        # Tableau des produits
        st.subheader("Détails des Produits")
        df_display = df_products[["product", "chiffre_affaires", "prix_moyen", "nb_ventes"]].copy()
        df_display.columns = ["Produit", "CA", "Prix Moyen", "Ventes"]
        st.dataframe(df_display, use_container_width=True)

# Footer
st.markdown("---")
st.caption("Dashboard alimenté par FastAPI + MongoDB | Data: Gold Layer")

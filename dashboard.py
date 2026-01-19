import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from io import BytesIO

from flows.config import BUCKET_GOLD, get_minio_client


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
        "🏠 Vue d'ensemble",
        "🌍 Analyse par Pays",
        "📦 Analyse Produits",
        "📅 Tendances Temporelles",
        "👥 Comportement Clients",
        "📊 Statistiques"
    ]
)

# === VUE D'ENSEMBLE ===
if page == "🏠 Vue d'ensemble":
    st.header("Vue d'ensemble des KPIs")
    
    # Charger les KPIs globaux
    kpis = load_gold_data("kpi_global.parquet")
    
    # Afficher les KPIs en colonnes
    financial_kpis = kpis[kpis["type"] == "financial"]
    volume_kpis = kpis[kpis["type"] == "volume"]
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        revenue = financial_kpis[financial_kpis["metric"] == "Total Revenue (€)"]["value"].iloc[0]
        st.metric("Chiffre d'Affaires", f"{revenue:,.0f} €")
    
    with col2:
        transactions = volume_kpis[volume_kpis["metric"] == "Total Transactions"]["value"].iloc[0]
        st.metric("Transactions", f"{transactions:,.0f}")
    
    with col3:
        avg_value = financial_kpis[financial_kpis["metric"] == "Average Transaction Value (€)"]["value"].iloc[0]
        st.metric("Panier Moyen", f"{avg_value:,.2f} €")
    
    with col4:
        clients = kpis[kpis["metric"] == "Total Unique Clients"]["value"].iloc[0]
        st.metric("Clients", f"{clients:,.0f}")
    
    st.markdown("---")
    
    # Charger les données mensuelles pour le graphique
    monthly = load_gold_data("agg_by_month.parquet")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Graphique CA mensuel
        fig = px.line(
            monthly,
            x="year_month",
            y="monthly_revenue",
            title="📈 Évolution du CA Mensuel",
            labels={"year_month": "Mois", "monthly_revenue": "CA (€)"}
        )
        fig.update_traces(line_color="#1f77b4", line_width=3)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Graphique croissance MoM
        fig = px.bar(
            monthly,
            x="year_month",
            y="revenue_mom_growth_pct",
            title="📊 Croissance MoM (%)",
            labels={"year_month": "Mois", "revenue_mom_growth_pct": "Croissance (%)"}
        )
        fig.add_hline(y=0, line_dash="dash", line_color="red")
        st.plotly_chart(fig, use_container_width=True)

# === ANALYSE PAR PAYS ===
elif page == "🌍 Analyse par Pays":
    st.header("Analyse par Pays")
    
    country = load_gold_data("agg_by_country.parquet")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Top pays par CA
        fig = px.bar(
            country.head(10),
            x="country",
            y="total_revenue",
            title="🏆 Top 10 Pays par CA",
            labels={"country": "Pays", "total_revenue": "CA (€)"},
            color="total_revenue",
            color_continuous_scale="Blues"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Part de marché par pays
        fig = px.pie(
            country.head(10),
            values="total_revenue",
            names="country",
            title="🥧 Répartition du CA par Pays"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Tableau détaillé
    st.subheader("📋 Détails par Pays")
    st.dataframe(
        country.style.format({
            "total_revenue": "{:,.2f} €",
            "avg_transaction_value": "{:,.2f} €",
            "revenue_share_pct": "{:.2f}%"
        }),
        use_container_width=True,
        height=400
    )

# === ANALYSE PRODUITS ===
elif page == "📦 Analyse Produits":
    st.header("Analyse des Produits")
    
    products = load_gold_data("agg_by_product.parquet")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # CA par produit
        fig = px.bar(
            products,
            x="product",
            y="total_revenue",
            title="💰 CA par Produit",
            labels={"product": "Produit", "total_revenue": "CA (€)"},
            color="total_revenue",
            color_continuous_scale="Greens"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Volume de ventes
        fig = px.bar(
            products,
            x="product",
            y="total_sales",
            title="📊 Volume de Ventes",
            labels={"product": "Produit", "total_sales": "Unités vendues"},
            color="total_sales",
            color_continuous_scale="Oranges"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Matrice pays-produits
    st.subheader("🌍 Matrice Pays × Produits")
    matrix = load_gold_data("matrix_country_product.parquet")
    
    # Créer un heatmap
    fig = px.imshow(
        matrix.set_index("country"),
        title="Heatmap CA par Pays et Produit",
        labels=dict(x="Produit", y="Pays", color="CA (€)"),
        color_continuous_scale="YlOrRd"
    )
    st.plotly_chart(fig, use_container_width=True)

# === TENDANCES TEMPORELLES ===
elif page == "📅 Tendances Temporelles":
    st.header("Tendances Temporelles")
    
    # Sélecteur de granularité
    granularity = st.radio(
        "Granularité",
        ["Jour", "Semaine", "Mois", "Trimestre"],
        horizontal=True
    )
    
    if granularity == "Jour":
        data = load_gold_data("agg_by_day.parquet")
        x_col = "date"
        y_col = "daily_revenue"
        ma_col = "revenue_ma7"
    elif granularity == "Semaine":
        data = load_gold_data("agg_by_week.parquet")
        x_col = "year_week"
        y_col = "weekly_revenue"
        ma_col = None
    elif granularity == "Mois":
        data = load_gold_data("agg_by_month.parquet")
        x_col = "year_month"
        y_col = "monthly_revenue"
        ma_col = None
    else:  # Trimestre
        data = load_gold_data("agg_by_quarter.parquet")
        x_col = "year_quarter"
        y_col = "quarterly_revenue"
        ma_col = None
    
    # Graphique principal
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=data[x_col],
        y=data[y_col],
        mode='lines+markers',
        name='CA',
        line=dict(color='#1f77b4', width=2)
    ))
    
    if ma_col and ma_col in data.columns:
        fig.add_trace(go.Scatter(
            x=data[x_col],
            y=data[ma_col],
            mode='lines',
            name='Moyenne mobile 7j',
            line=dict(color='red', width=2, dash='dash')
        ))
    
    fig.update_layout(
        title=f"📈 Évolution du CA ({granularity})",
        xaxis_title="Période",
        yaxis_title="CA (€)",
        hovermode='x unified'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Afficher le tableau
    st.dataframe(data, use_container_width=True, height=400)

# === COMPORTEMENT CLIENTS ===
elif page == "👥 Comportement Clients":
    st.header("Comportement Clients")
    
    clients = load_gold_data("client_behavior_rfm.parquet")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Clients", f"{len(clients):,}")
    with col2:
        vip_count = len(clients[clients["client_segment"] == "VIP"])
        st.metric("Clients VIP", f"{vip_count:,}")
    with col3:
        avg_spent = clients["total_spent"].mean()
        st.metric("Dépense Moyenne", f"{avg_spent:,.2f} €")
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Répartition des segments
        segment_counts = clients["client_segment"].value_counts().reset_index()
        segment_counts.columns = ["segment", "count"]
        
        fig = px.pie(
            segment_counts,
            values="count",
            names="segment",
            title="🎯 Répartition des Segments Clients"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Distribution des dépenses
        fig = px.histogram(
            clients,
            x="total_spent",
            nbins=50,
            title="💰 Distribution des Dépenses Clients",
            labels={"total_spent": "Dépense Totale (€)"}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Top clients
    st.subheader("🏆 Top 20 Clients")
    top_clients = clients.head(20)[["name", "country", "total_spent", "total_purchases", "client_segment"]]
    st.dataframe(
        top_clients.style.format({
            "total_spent": "{:,.2f} €"
        }),
        use_container_width=True
    )

# === STATISTIQUES ===
elif page == "📊 Statistiques":
    st.header("Statistiques et Distributions")
    
    stats = load_gold_data("statistical_distributions.parquet")
    
    st.subheader("📈 Distribution des Montants de Transaction")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Moyenne", f"{stats['mean'].iloc[0]:,.2f} €")
        st.metric("Médiane", f"{stats['median'].iloc[0]:,.2f} €")
    
    with col2:
        st.metric("Écart-type", f"{stats['std'].iloc[0]:,.2f} €")
        st.metric("Min", f"{stats['min'].iloc[0]:,.2f} €")
    
    with col3:
        st.metric("Max", f"{stats['max'].iloc[0]:,.2f} €")
        st.metric("Q25", f"{stats['q25'].iloc[0]:,.2f} €")
    
    with col4:
        st.metric("Q75", f"{stats['q75'].iloc[0]:,.2f} €")
        st.metric("IQR", f"{stats['q75'].iloc[0] - stats['q25'].iloc[0]:,.2f} €")
    
    st.markdown("---")
    
    # Charger la table de faits pour les distributions
    fact_table = load_gold_data("fact_purchases.parquet")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Box plot des montants
        fig = px.box(
            fact_table,
            y="amount",
            title="📦 Box Plot - Montants",
            labels={"amount": "Montant (€)"}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Distribution des montants
        fig = px.histogram(
            fact_table,
            x="amount",
            nbins=100,
            title="📊 Distribution des Montants",
            labels={"amount": "Montant (€)"}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Statistiques détaillées
    st.subheader("📋 Tableau Statistique Complet")
    st.dataframe(stats.T, use_container_width=True)


# Footer
st.sidebar.markdown("---")
st.sidebar.info("""
    **Sources de données:**
    - 13 fichiers Parquet dans MinIO (bucket: gold)
    - Pipeline: Bronze → Silver → Gold
""")

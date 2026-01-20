"""
API FastAPI pour exposer les données MongoDB
"""
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))
from utils.config import (
    get_mongodb_database,
    COLLECTION_CLIENTS,
    COLLECTION_PRODUCTS,
    COLLECTION_MONTHLY_SALES,
    COLLECTION_METADATA
)

app = FastAPI(
    title="Analytics API",
    description="API pour exposer les données analytics depuis MongoDB",
    version="1.0.0"
)

# CORS middleware pour permettre les requêtes depuis Streamlit
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def serialize_doc(doc: dict) -> dict:
    """Convertir ObjectId en string pour la sérialisation JSON"""
    if doc and "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc


# ============================================
# ENDPOINTS CLIENTS
# ============================================

@app.get("/clients", tags=["Clients"])
def get_clients(
    country: Optional[str] = Query(None, description="Filtrer par pays"),
    min_total: Optional[float] = Query(None, description="Montant total minimum"),
    limit: int = Query(100, le=1000, description="Nombre max de résultats"),
    skip: int = Query(0, ge=0, description="Nombre de résultats à sauter")
):
    """Récupérer la liste des clients avec filtres optionnels"""
    db = get_mongodb_database()
    collection = db[COLLECTION_CLIENTS]

    query = {}
    if country:
        query["country"] = country
    if min_total is not None:
        query["total_achats"] = {"$gte": min_total}

    cursor = collection.find(query).skip(skip).limit(limit)
    clients = [serialize_doc(doc) for doc in cursor]

    return {"data": clients, "count": len(clients)}


@app.get("/clients/{client_id}", tags=["Clients"])
def get_client_by_id(client_id: str):
    """Récupérer un client par son ID"""
    db = get_mongodb_database()
    collection = db[COLLECTION_CLIENTS]

    client = collection.find_one({"client_id": client_id})
    if not client:
        raise HTTPException(status_code=404, detail="Client non trouvé")

    return serialize_doc(client)


@app.get("/clients/stats/by-country", tags=["Clients"])
def get_clients_stats_by_country():
    """Statistiques agrégées par pays"""
    db = get_mongodb_database()
    collection = db[COLLECTION_CLIENTS]

    pipeline = [
        {
            "$group": {
                "_id": "$country",
                "total_clients": {"$sum": 1},
                "total_ca": {"$sum": "$total_achats"},
                "ca_moyen": {"$avg": "$total_achats"},
                "panier_moyen": {"$avg": "$panier_moyen"}
            }
        },
        {"$sort": {"total_ca": -1}}
    ]

    results = list(collection.aggregate(pipeline))
    for r in results:
        r["country"] = r.pop("_id")
        r["total_ca"] = round(r["total_ca"], 2)
        r["ca_moyen"] = round(r["ca_moyen"], 2)
        r["panier_moyen"] = round(r["panier_moyen"], 2)

    return {"data": results}


@app.get("/clients/top/{n}", tags=["Clients"])
def get_top_clients(n: int = 10):
    """Récupérer les top N clients par chiffre d'affaires"""
    db = get_mongodb_database()
    collection = db[COLLECTION_CLIENTS]

    cursor = collection.find().sort("total_achats", -1).limit(n)
    clients = [serialize_doc(doc) for doc in cursor]

    return {"data": clients}


# ============================================
# ENDPOINTS PRODUITS
# ============================================

@app.get("/products", tags=["Produits"])
def get_products():
    """Récupérer tous les produits avec leurs statistiques"""
    db = get_mongodb_database()
    collection = db[COLLECTION_PRODUCTS]

    products = [serialize_doc(doc) for doc in collection.find().sort("chiffre_affaires", -1)]
    return {"data": products, "count": len(products)}


@app.get("/products/{product_name}", tags=["Produits"])
def get_product_by_name(product_name: str):
    """Récupérer un produit par son nom"""
    db = get_mongodb_database()
    collection = db[COLLECTION_PRODUCTS]

    product = collection.find_one({"product": product_name})
    if not product:
        raise HTTPException(status_code=404, detail="Produit non trouvé")

    return serialize_doc(product)


# ============================================
# ENDPOINTS VENTES MENSUELLES
# ============================================

@app.get("/sales/monthly", tags=["Ventes"])
def get_monthly_sales():
    """Récupérer les ventes mensuelles"""
    db = get_mongodb_database()
    collection = db[COLLECTION_MONTHLY_SALES]

    sales = [serialize_doc(doc) for doc in collection.find().sort("mois", 1)]
    return {"data": sales, "count": len(sales)}


@app.get("/sales/monthly/{month}", tags=["Ventes"])
def get_sales_by_month(month: str):
    """Récupérer les ventes d'un mois spécifique (format: YYYY-MM)"""
    db = get_mongodb_database()
    collection = db[COLLECTION_MONTHLY_SALES]

    sale = collection.find_one({"mois": month})
    if not sale:
        raise HTTPException(status_code=404, detail="Données non trouvées pour ce mois")

    return serialize_doc(sale)


@app.get("/sales/summary", tags=["Ventes"])
def get_sales_summary():
    """Résumé global des ventes"""
    db = get_mongodb_database()
    collection = db[COLLECTION_MONTHLY_SALES]

    pipeline = [
        {
            "$group": {
                "_id": None,
                "total_ca": {"$sum": "$chiffre_affaires"},
                "total_achats": {"$sum": "$nb_achats"},
                "panier_moyen_global": {"$avg": "$panier_moyen"},
                "nb_mois": {"$sum": 1}
            }
        }
    ]

    results = list(collection.aggregate(pipeline))
    if results:
        summary = results[0]
        summary.pop("_id")
        summary["total_ca"] = round(summary["total_ca"], 2)
        summary["panier_moyen_global"] = round(summary["panier_moyen_global"], 2)
        return summary

    return {"total_ca": 0, "total_achats": 0, "panier_moyen_global": 0, "nb_mois": 0}


# ============================================
# ENDPOINTS METADATA / REFRESH
# ============================================

@app.get("/metadata/refresh", tags=["Metadata"])
def get_refresh_info():
    """Récupérer les informations du dernier refresh"""
    db = get_mongodb_database()
    collection = db[COLLECTION_METADATA]

    info = collection.find_one({"_id": "refresh_info"})
    if not info:
        raise HTTPException(status_code=404, detail="Aucune information de refresh disponible")

    info["_id"] = str(info["_id"])
    return info


@app.get("/health", tags=["Health"])
def health_check():
    """Vérifier l'état de l'API et de la connexion MongoDB"""
    try:
        db = get_mongodb_database()
        db.command("ping")
        return {"status": "healthy", "mongodb": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"MongoDB unavailable: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

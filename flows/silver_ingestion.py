from io import BytesIO
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
from typing import Dict, Any

from prefect import flow, task

from config import BUCKET_SILVER, BUCKET_SOURCES, get_minio_client

@task(name="upload_to_sources", retries=2)
def upload_csv_to_souces(file_path: str, object_name: str) -> str:
    """
    Upload local CSV file to MinIO sources bucket.

    Args:
        file_path: Path to local CSV file
        object_name: Name of object in MinIO

    Returns:
        Object name in MinIO
    """

    client = get_minio_client()

    if not client.bucket_exists(BUCKET_SOURCES):
        client.make_bucket(BUCKET_SOURCES)

    client.fput_object(BUCKET_SOURCES, object_name, file_path)
    print(f"Uploaded {object_name} to {BUCKET_SOURCES}")
    return object_name

@task(name="clean_data", retries=2)
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoyer les données : valeurs nulles et aberrantes.
    
    Args:
        df: DataFrame à nettoyer
        
    Returns:
        DataFrame nettoyé
    """
    print(f"Nettoyage: {len(df)} lignes avant nettoyage")
    
    # Supprimer les lignes entièrement vides
    df = df.dropna(how='all')
    
    # Remplacer les valeurs vides par NaN pour un traitement cohérent
    df = df.replace(['', ' ', 'null', 'NULL', 'None'], np.nan)
    
    # Pour les colonnes numériques, supprimer les valeurs négatives aberrantes
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if 'montant' in col.lower() or 'price' in col.lower() or 'amount' in col.lower():
            df = df[df[col] >= 0]  # Montants négatifs = aberrants
            
    # Supprimer les emails invalides
    email_cols = [col for col in df.columns if 'email' in col.lower()]
    for col in email_cols:
        df = df[df[col].str.contains('@', na=False)]
    
    # Valider les produits contre la liste autorisée
    products = ["Laptop", "Phone", "Tablet", "Headphones", "Monitor", "Keyboard", "Mouse", "Webcam", "Speaker", "Charger"]
    product_cols = [col for col in df.columns if 'product' in col.lower() or 'produit' in col.lower()]
    for col in product_cols:
        df = df[df[col].isin(products)]
        print(f"Validation produits: produits autorisés uniquement pour {col}")
    
    print(f"Nettoyage: {len(df)} lignes après nettoyage")
    return df

@task(name="standardize_dates", retries=2)
def standardize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardiser les formats de dates.
    
    Args:
        df: DataFrame avec des colonnes de dates
        
    Returns:
        DataFrame avec dates standardisées
    """
    date_cols = [col for col in df.columns if 'date' in col.lower()]
    
    for col in date_cols:
        try:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            # Formatage ISO standardisé
            df[col] = df[col].dt.strftime('%Y-%m-%d')
            print(f"Standardisation date pour {col}")
        except Exception as e:
            print(f"Erreur standardisation {col}: {e}")
            
    return df

@task(name="normalize_types", retries=2) 
def normalize_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliser les types de données.
    
    Args:
        df: DataFrame à normaliser
        
    Returns:
        DataFrame avec types normalisés
    """
    # Normaliser les colonnes texte
    text_cols = df.select_dtypes(include=['object']).columns
    for col in text_cols:
        if 'email' not in col.lower():
            df[col] = df[col].astype(str).str.strip().str.title()
            
    # Normaliser les IDs en entiers
    id_cols = [col for col in df.columns if 'id' in col.lower()]
    for col in id_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        
    # Normaliser les montants en float
    amount_cols = [col for col in df.columns if 'montant' in col.lower() or 'amount' in col.lower()]
    for col in amount_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
        
    print("Types de données normalisés")
    return df

@task(name="deduplicate_data", retries=2)
def deduplicate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Dédupliquer les enregistrements.
    
    Args:
        df: DataFrame à dédupliquer
        
    Returns:
        DataFrame dédupliqué
    """
    initial_count = len(df)
    
    # Dédupliquer en gardant la première occurrence
    df = df.drop_duplicates(keep='first')
    
    final_count = len(df)
    removed_count = initial_count - final_count
    
    print(f"Déduplication: {removed_count} doublons supprimés ({final_count} lignes restantes)")
    return df

@task(name="save_to_silver", retries=2)
def save_to_silver(df: pd.DataFrame, object_name: str) -> str:
    """
    Sauvegarder les données nettoyées dans la couche silver.
    
    Args:
        df: DataFrame nettoyé
        object_name: Nom du fichier
        
    Returns:
        Nom de l'objet sauvegardé
    """
    client = get_minio_client()
    
    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)
    
    # Convertir en CSV
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()
    
    client.put_object(
        BUCKET_SILVER,
        object_name,
        BytesIO(csv_data),
        length=len(csv_data)
    )
    
    print(f"Saved {object_name} to {BUCKET_SILVER} with {len(df)} rows")
    return object_name

@flow(name="Silver Transformation Flow")
def silver_transformation_flow(data_dir: str = "./data/sources") -> Dict[str, Any]:
    """
    Flow principal pour les transformations silver.
    
    Transformations appliquées:
    - Nettoyage des valeurs nulles et aberrantes
    - Standardisation des formats de dates
    - Normalisation des types de données
    - Déduplication des enregistrements

    Args:
        data_dir: Répertoire contenant les fichiers CSV sources

    Returns:
        Dictionnaire avec les résultats de transformation
    """
    data_path = Path(data_dir)
    files_to_process = [
        ("clients.csv", str(data_path / "clients.csv")),
        ("achats.csv", str(data_path / "achats.csv"))
    ]
    
    results = {}
    
    for file_name, file_path in files_to_process:
        print(f"\n=== Traitement de {file_name} ===")
        
        raw_df = read_source_data(file_path)
        
        # Clean values
        cleaned_df = clean_data(raw_df)
        
        # Dates
        dated_df = standardize_dates(cleaned_df)
        
        # Types
        typed_df = normalize_types(dated_df)
        
        # Doublons
        final_df = deduplicate_data(typed_df)
        
        # Sauvegarde
        silver_name = save_to_silver(final_df, file_name)
        
        results[file_name] = {
            "silver_file": silver_name,
            "original_rows": len(raw_df),
            "final_rows": len(final_df),
            "quality_score": len(final_df) / len(raw_df) if len(raw_df) > 0 else 0
        }
    
    return results

if __name__ == "__main__":
    result = silver_transformation_flow()
    print(f"\nSilver transformation complete: {result}")
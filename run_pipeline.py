#!/usr/bin/env python3
"""
Script principal pour exécuter le pipeline complet de données.
Ordre d'exécution :
1. Génération des données (clients et achats)
2. Bronze ingestion
3. Silver ingestion  
4. Gold ingestion
"""

import subprocess
import sys
import os
from pathlib import Path

def run_command(command: str, description: str) -> bool:
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            check=True, 
            text=True,
            capture_output=True
        )
        
        if result.stdout:
            print(result.stdout)
        
        if result.stderr:
            print("⚠️ STDERR:")
            print(result.stderr)
            
        print(f"{description} - TERMINÉ AVEC SUCCÈS")
        return True
        
    except subprocess.CalledProcessError as e:
        if e.stdout:
            print(f"STDOUT: {e.stdout}")
        if e.stderr:
            print(f"STDERR: {e.stderr}")
        return False

def main():
    # Liste des étapes du pipeline
    steps = [
        {
            "command": "python3 scripts/generate_data_clients.py",
            "description": "Génération des données clients"
        },
        {
            "command": "python3 scripts/generate_data_purchases.py", 
            "description": "Génération des données achats"
        },
        {
            "command": "python3 flows/bronze_ingestion.py",
            "description": "Bronze Ingestion"
        },
        {
            "command": "python3 flows/silver_ingestion.py",
            "description": "Silver Ingestion"
        },
        {
            "command": "python3 flows/gold_ingestion.py",
            "description": "Gold Ingestion"
        }
    ]
    
    # Exécuter chaque étape
    success_count = 0
    total_steps = len(steps)
    
    for i, step in enumerate(steps, 1):
        
        if run_command(step["command"], step["description"]):
            success_count += 1
        else:
            sys.exit(1)

if __name__ == "__main__":
    # Vérifier que nous sommes dans le bon répertoire
    if not Path("flows").exists() or not Path("scripts").exists():
        sys.exit(1)
    
    main()
#!/usr/bin/env python3
"""
Script principal pour exécuter le pipeline complet de données.
Compare les performances entre Pandas et Spark.
"""

import subprocess
import sys
import time
from pathlib import Path


def run_command(command: str, description: str) -> tuple[bool, float]:
    """Exécute une commande et retourne (succès, durée en secondes)"""
    start_time = time.time()
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            text=True,
            capture_output=True
        )
        elapsed = time.time() - start_time

        if result.stdout:
            print(result.stdout)

        return True, elapsed

    except subprocess.CalledProcessError as e:
        elapsed = time.time() - start_time
        if e.stdout:
            print(f"STDOUT: {e.stdout}")
        if e.stderr:
            print(f"STDERR: {e.stderr}")
        return False, elapsed


def print_header(title: str):
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def print_comparison(pandas_time: float, spark_time: float, step_name: str):
    """Affiche la comparaison des temps d'exécution"""
    print("\n" + "-" * 40)
    print(f"  COMPARAISON - {step_name}")
    print("-" * 40)
    print(f"  Pandas : {pandas_time:.2f}s")
    print(f"  Spark  : {spark_time:.2f}s")

    if pandas_time < spark_time:
        diff = spark_time - pandas_time
        pct = (diff / spark_time) * 100
        print(f"  --> Pandas plus rapide de {diff:.2f}s ({pct:.1f}%)")
    else:
        diff = pandas_time - spark_time
        pct = (diff / pandas_time) * 100
        print(f"  --> Spark plus rapide de {diff:.2f}s ({pct:.1f}%)")
    print("-" * 40)


def main():
    results = {
        "pandas": {"silver": 0, "gold": 0},
        "spark": {"silver": 0, "gold": 0}
    }

    # === ÉTAPE 1: Génération des données ===
    print_header("GÉNÉRATION DES DONNÉES")

    success, _ = run_command("python3 scripts/generate_data_clients.py", "Génération clients")
    if not success:
        sys.exit(1)
    print("Clients générés")

    success, _ = run_command("python3 scripts/generate_data_purchases.py", "Génération achats")
    if not success:
        sys.exit(1)
    print("Achats générés")

    # === ÉTAPE 2: Bronze Ingestion ===
    print_header("BRONZE INGESTION")

    success, elapsed = run_command("python3 flows/bronze_ingestion.py", "Bronze")
    if not success:
        sys.exit(1)
    print(f"Bronze terminé en {elapsed:.2f}s")

    # === ÉTAPE 3: Silver Ingestion - COMPARAISON ===
    print_header("SILVER INGESTION - PANDAS")

    success, pandas_silver = run_command("python3 flows/silver_ingestion.py", "Silver Pandas")
    if not success:
        sys.exit(1)
    results["pandas"]["silver"] = pandas_silver
    print(f"Silver (Pandas) terminé en {pandas_silver:.2f}s")

    print_header("SILVER INGESTION - SPARK")

    success, spark_silver = run_command("python3 flows/silver_ingestion_spark.py", "Silver Spark")
    if not success:
        print("Spark Silver a échoué - continuons avec Pandas uniquement")
        spark_silver = 0
    else:
        results["spark"]["silver"] = spark_silver
        print(f"Silver (Spark) terminé en {spark_silver:.2f}s")

    if spark_silver > 0:
        print_comparison(pandas_silver, spark_silver, "SILVER")

    # === ÉTAPE 4: Gold Ingestion - COMPARAISON ===
    print_header("GOLD INGESTION - PANDAS")

    success, pandas_gold = run_command("python3 flows/gold_ingestion.py", "Gold Pandas")
    if not success:
        sys.exit(1)
    results["pandas"]["gold"] = pandas_gold
    print(f"Gold (Pandas) terminé en {pandas_gold:.2f}s")

    print_header("GOLD INGESTION - SPARK")

    success, spark_gold = run_command("python3 flows/gold_ingestion_spark.py", "Gold Spark")
    if not success:
        print("Spark Gold a échoué - continuons avec Pandas uniquement")
        spark_gold = 0
    else:
        results["spark"]["gold"] = spark_gold
        print(f"Gold (Spark) terminé en {spark_gold:.2f}s")

    if spark_gold > 0:
        print_comparison(pandas_gold, spark_gold, "GOLD")

    # === RÉSUMÉ FINAL ===
    print_header("RÉSUMÉ FINAL")

    total_pandas = results["pandas"]["silver"] + results["pandas"]["gold"]
    total_spark = results["spark"]["silver"] + results["spark"]["gold"]

    print(f"""
    +----------------+----------+----------+
    |     Étape      |  Pandas  |  Spark   |
    +----------------+----------+----------+
    | Silver         | {results['pandas']['silver']:>6.2f}s  | {results['spark']['silver']:>6.2f}s  |
    | Gold           | {results['pandas']['gold']:>6.2f}s  | {results['spark']['gold']:>6.2f}s  |
    +----------------+----------+----------+
    | TOTAL          | {total_pandas:>6.2f}s  | {total_spark:>6.2f}s  |
    +----------------+----------+----------+
    """)

    if total_spark > 0:
        if total_pandas < total_spark:
            print(f"  Pandas est plus rapide de {total_spark - total_pandas:.2f}s")
            print("  (Normal avec peu de données - overhead Spark)")
        else:
            print(f"  Spark est plus rapide de {total_pandas - total_spark:.2f}s")


if __name__ == "__main__":
    if not Path("flows").exists() or not Path("scripts").exists():
        sys.exit(1)

    main()

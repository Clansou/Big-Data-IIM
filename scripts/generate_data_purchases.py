import csv
import random
import pandas as pd

from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

def generate_clients(n_clients: int, output_path: Path) -> list[int]:
    """
    Generate fake Client data

    Args:
        client_ids (int): Number of clients to generate
        id_achat, id_client, date_achat, montant_achat, produit
        output_path (Path): Path to save the generated CSV file

        Returns:
            list[int]: List of generated client IDs
    """

    products = ["Laptop", "Phone", "Tablet", "Headphones", "Monitor", "Keyboard", "Mouse", "Webcam", "Speaker", "Charger"]

    data_path = Path("./data/sources")
    clients_file = str(data_path / "clients.csv")

    # Lire les clients existants pour obtenir les client_ids valides
    df_clients = pd.read_csv(clients_file)
    client_ids = df_clients['client_id'].tolist()   
    
    # Liste pour stocker les achats générés
    purchases = [] 


    for i in range(1, n_clients + 1):
        data_achat = fake.date_between(start_date='-3y', end_date='-1m')
        client_id = random.choice(client_ids)
        purchases.append(
            {
                "purchase_id": i,
                "client_id": client_id, 
                "date_purchase": data_achat.strftime("%Y-%m-%d"),
                "amount": round(random.uniform(10.0, 1000.0), 2),
                "product": random.choice(products),
            }
        )
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["purchase_id", "client_id", "date_purchase", "amount", "product"])
        writer.writeheader()
        writer.writerows(purchases)

    print(f"Generated clients: ({n_clients}) at {output_path}")
    return client_ids

if __name__ == "__main__":
    output_dir = Path(__file__).parent.parent / "data" / "sources"

    achats_ids = generate_clients(
        n_clients=1000000,
        output_path=output_dir / "purchases.csv"
    )


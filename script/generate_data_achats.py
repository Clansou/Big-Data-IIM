import csv
import random

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

    clients = []
    client_ids = []

    for i in range(1, n_clients + 1):
        data_achat = fake.date_between(start_date='-3y', end_date='-1m')
        clients.append(
            {
                "id_achat": i,
                "client_id": i,
                "date_achat": data_achat.strftime("%Y-%m-%d"),
                "montant_achat": round(random.uniform(10.0, 1000.0), 2),
                "produit": random.choice(products),
            }
        )

        client_ids.append(i)
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["id_achat", "client_id", "date_achat", "montant_achat", "produit"])
        writer.writeheader()
        writer.writerows(clients)

    print(f"Generated clients: ({n_clients}) at {output_path}")
    return client_ids

if __name__ == "__main__":
    output_dir = Path(__file__).parent.parent / "data" / "sources"

    achats_ids = generate_clients(
        n_clients=1500,
        output_path=output_dir / "achats.csv"
    )


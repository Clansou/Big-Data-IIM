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
        n_clients (int): Number of clients to generate
        output_path (Path): Path to save the generated CSV file

        Returns:
            list[int]: List of generated client IDs
    """

    countries = ["USA", "Canada", "UK", "Germany", "France", "Australia"]

    clients = []
    client_ids = []

    for i in range(1, n_clients + 1):
        data_inscription = fake.date_between(start_date='-3y', end_date='-1m')
        clients.append(
            {
                "client_id": i,
                "name": fake.name(),
                "email": fake.email(),
                "date_inscription": data_inscription.strftime("%Y-%m-%d"),
                "country": random.choice(countries),
            }
        )

        client_ids.append(i)
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["client_id", "name", "email", "date_inscription", "country"])
        writer.writeheader()
        writer.writerows(clients)

    print(f"Generated clients: ({n_clients}) at {output_path}")
    return client_ids

if __name__ == "__main__":
    output_dir = Path(__file__).parent.parent / "data" / "sources"

    clients_ids = generate_clients(
        n_clients=1500,
        output_path=output_dir / "clients.csv"
    )


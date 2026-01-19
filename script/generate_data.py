import csv
import random

import datetime from datetime, timedelta
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
        client_id = i
        name = fake.name()
        email = fake.email()
        country = random.choice(countries)

        clients.append({
            "client_id": client_id,
            "name": name,
            "email": email,
            "country": country
        })
        client_ids.append(client_id)
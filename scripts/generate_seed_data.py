"""Gera os CSVs seed dos dois domínios do data mesh (determinístico).

Domínios:
  - marketing/campaigns.csv  (produzido pelo time de Marketing)
  - sales/orders.csv         (produzido pelo time de Vendas)

Os dois são consultados em conjunto pelo Trino (cross-domain JOIN) SEM cópia.
Rode:  python scripts/generate_seed_data.py
"""
from __future__ import annotations

import csv
import os
import random
from datetime import datetime, timedelta

random.seed(42)  # reprodutível

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")

CHANNELS = ["paid_search", "social", "email", "display", "affiliate"]
PRODUCTS = ["keyboard", "mouse", "monitor", "webcam", "headset", "dock", "stand"]
COUNTRIES = ["BR", "US", "PT", "DE", "MX", "AR"]

CAMPAIGNS = [
    ("CMP-001", "Summer Launch",   "paid_search", 50000),
    ("CMP-002", "Back to Work",    "social",      30000),
    ("CMP-003", "Black Friday",    "email",       80000),
    ("CMP-004", "New Year Deals",  "display",     25000),
    ("CMP-005", "Spring Refresh",  "affiliate",   15000),
    ("CMP-006", "Pro Bundle",      "paid_search", 45000),
    ("CMP-007", "Student Promo",   "social",      12000),
    ("CMP-008", "Loyalty Rewards", "email",       20000),
]


def write_campaigns() -> None:
    path = os.path.join(DATA, "marketing", "campaigns.csv")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    base = datetime(2024, 1, 1)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["campaign_id", "campaign_name", "channel", "start_date", "end_date", "budget"])
        for i, (cid, name, channel, budget) in enumerate(CAMPAIGNS):
            start = base + timedelta(days=i * 30)
            end = start + timedelta(days=45)
            w.writerow([cid, name, channel, start.date(), end.date(), budget])
    print(f"escrito {path} ({len(CAMPAIGNS)} campanhas)")


def write_orders(n: int = 400) -> None:
    path = os.path.join(DATA, "sales", "orders.csv")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    base = datetime(2024, 1, 5, 8, 0, 0)
    campaign_ids = [c[0] for c in CAMPAIGNS]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            ["order_id", "order_ts", "customer_id", "campaign_id",
             "product", "quantity", "unit_price", "amount", "country"]
        )
        for i in range(1, n + 1):
            ts = base + timedelta(minutes=random.randint(0, 60 * 24 * 250))
            qty = random.randint(1, 5)
            unit = round(random.uniform(19.9, 499.9), 2)
            amount = round(qty * unit, 2)
            w.writerow([
                f"ORD-{i:05d}",
                ts.strftime("%Y-%m-%d %H:%M:%S"),
                f"CUST-{random.randint(1, 120):04d}",
                random.choice(campaign_ids),
                random.choice(PRODUCTS),
                qty,
                unit,
                amount,
                random.choice(COUNTRIES),
            ])
    print(f"escrito {path} ({n} pedidos)")


if __name__ == "__main__":
    write_campaigns()
    write_orders()

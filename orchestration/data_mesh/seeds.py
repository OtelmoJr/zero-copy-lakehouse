"""Carrega os CSVs seed dos domínios como DataFrames pandas."""
from __future__ import annotations

import os

import pandas as pd

DATA_DIR = os.getenv("SEED_DATA_DIR", "/opt/dagster/app/data")


def load_orders() -> pd.DataFrame:
    return pd.read_csv(os.path.join(DATA_DIR, "sales", "orders.csv"))


def load_campaigns() -> pd.DataFrame:
    return pd.read_csv(os.path.join(DATA_DIR, "marketing", "campaigns.csv"))


def df_to_records(df: pd.DataFrame, columns: list[str]) -> list[tuple]:
    return list(df[columns].itertuples(index=False, name=None))

"""Definição das tabelas Iceberg dos domínios + specs de colunas para INSERT.

Domínios (cada schema = um domínio do data mesh):
  - sales.orders            (particionado por day(order_ts) — hidden partitioning)
  - marketing.campaigns
  - analytics.revenue_by_campaign  (gold: cross-domain JOIN, zero-copy)
"""
from __future__ import annotations

from data_mesh.trino_io import TrinoClient

# --- specs usados pelo INSERT (nome + tipo lógico para gerar literais SQL) ---
ORDERS_COLUMNS = [
    "order_id", "order_ts", "customer_id", "campaign_id",
    "product", "quantity", "unit_price", "amount", "country",
]
ORDERS_TYPES = ["str", "ts", "str", "str", "str", "int", "double", "double", "str"]

CAMPAIGNS_COLUMNS = ["campaign_id", "campaign_name", "channel", "start_date", "end_date", "budget"]
CAMPAIGNS_TYPES = ["str", "str", "str", "date", "date", "int"]

VALID_COUNTRIES = ["BR", "US", "PT", "DE", "MX", "AR"]


def ensure_orders_table(client: TrinoClient, catalog: str = "iceberg") -> None:
    client.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.sales", catalog)
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.sales.orders (
            order_id     varchar,
            order_ts     timestamp(6),
            customer_id  varchar,
            campaign_id  varchar,
            product      varchar,
            quantity     integer,
            unit_price   double,
            amount       double,
            country      varchar
        )
        WITH (
            partitioning = ARRAY['day(order_ts)'],
            format = 'PARQUET'
        )
        """,
        catalog,
    )


def ensure_campaigns_table(client: TrinoClient, catalog: str = "iceberg") -> None:
    client.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.marketing", catalog)
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.marketing.campaigns (
            campaign_id    varchar,
            campaign_name  varchar,
            channel        varchar,
            start_date     date,
            end_date       date,
            budget         integer
        )
        WITH (format = 'PARQUET')
        """,
        catalog,
    )


def ensure_analytics_schema(client: TrinoClient, catalog: str = "iceberg") -> None:
    client.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.analytics", catalog)


# Cross-domain JOIN: sales x marketing, SEM copiar dados entre domínios.
REVENUE_CTAS = """
CREATE OR REPLACE TABLE iceberg.analytics.revenue_by_campaign
WITH (format = 'PARQUET') AS
SELECT
    c.campaign_id,
    c.campaign_name,
    c.channel,
    c.budget,
    count(o.order_id)                              AS orders_count,
    COALESCE(sum(o.amount), 0.0)                   AS revenue,
    round(COALESCE(sum(o.amount), 0.0) / NULLIF(c.budget, 0), 3) AS roi
FROM iceberg.marketing.campaigns c
LEFT JOIN iceberg.sales.orders o
       ON o.campaign_id = c.campaign_id
GROUP BY c.campaign_id, c.campaign_name, c.channel, c.budget
ORDER BY revenue DESC
"""

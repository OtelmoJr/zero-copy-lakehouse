"""Camada GOLD — `analytics.revenue_by_campaign`.

A estrela do zero-copy: faz JOIN entre dois domínios (sales x marketing)
sem duplicar nenhum dado. Depende dos assets dos dois domínios.
"""
from __future__ import annotations

from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetKey,
    MaterializeResult,
    MetadataValue,
    asset,
)

from data_mesh.assets.marketing import CAMPAIGNS_KEY
from data_mesh.assets.sales import ORDERS_KEY
from data_mesh.resources import TrinoResource
from data_mesh.schema import REVENUE_CTAS, ensure_analytics_schema

REVENUE_KEY = AssetKey(["analytics", "revenue_by_campaign"])


def _markdown_table(columns: list[str], rows: list[tuple]) -> str:
    header = "| " + " | ".join(columns) + " |"
    sep = "| " + " | ".join("---" for _ in columns) + " |"
    body = "\n".join("| " + " | ".join(str(c) for c in r) + " |" for r in rows)
    return f"{header}\n{sep}\n{body}"


@asset(
    key=REVENUE_KEY,
    deps=[ORDERS_KEY, CAMPAIGNS_KEY],
    group_name="analytics_domain",
    description="Receita e ROI por campanha — cross-domain JOIN (zero-copy).",
    check_specs=[AssetCheckSpec(name="no_negative_revenue", asset=REVENUE_KEY)],
)
def revenue_by_campaign(trino: TrinoResource) -> MaterializeResult:
    client = trino.get_client()
    ensure_analytics_schema(client)
    client.execute(REVENUE_CTAS)

    cols, rows = client.execute(
        """
        SELECT campaign_id, campaign_name, channel, orders_count, revenue, roi
        FROM iceberg.analytics.revenue_by_campaign
        ORDER BY revenue DESC
        LIMIT 10
        """
    )
    negatives = int(
        client.scalar("SELECT count(*) FROM iceberg.analytics.revenue_by_campaign WHERE revenue < 0")
    )
    total = int(client.scalar("SELECT count(*) FROM iceberg.analytics.revenue_by_campaign"))

    return MaterializeResult(
        metadata={
            "campaigns": total,
            "top_10_by_revenue": MetadataValue.md(_markdown_table(cols, rows)),
        },
        check_results=[
            AssetCheckResult(check_name="no_negative_revenue", passed=(negatives == 0))
        ],
    )

"""Domínio MARKETING — asset `marketing.campaigns` com quality gate (GE)."""
from __future__ import annotations

from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetKey,
    MaterializeResult,
    MetadataValue,
    asset,
)

from data_mesh.quality.expectations import campaigns_expectations, summarize, validate
from data_mesh.resources import TrinoResource
from data_mesh.schema import CAMPAIGNS_COLUMNS, CAMPAIGNS_TYPES, ensure_campaigns_table
from data_mesh.seeds import df_to_records, load_campaigns

CAMPAIGNS_KEY = AssetKey(["marketing", "campaigns"])


@asset(
    key=CAMPAIGNS_KEY,
    group_name="marketing_domain",
    description="Campanhas do domínio Marketing. Tabela Iceberg.",
    check_specs=[AssetCheckSpec(name="great_expectations", asset=CAMPAIGNS_KEY, blocking=True)],
)
def marketing_campaigns(trino: TrinoResource) -> MaterializeResult:
    client = trino.get_client()
    df = load_campaigns()

    result = validate(df, "campaigns", campaigns_expectations())
    summary = summarize(result)
    passed = bool(result.success)

    if not passed:
        return MaterializeResult(
            metadata={
                "rows_candidate": len(df),
                "written_to_main": False,
                "great_expectations": MetadataValue.json(summary),
            },
            check_results=[
                AssetCheckResult(
                    check_name="great_expectations",
                    passed=False,
                    metadata={"great_expectations": MetadataValue.json(summary)},
                )
            ],
        )

    ensure_campaigns_table(client)
    client.execute("DELETE FROM iceberg.marketing.campaigns")
    records = df_to_records(df, CAMPAIGNS_COLUMNS)
    client.insert("iceberg.marketing.campaigns", CAMPAIGNS_COLUMNS, CAMPAIGNS_TYPES, records)
    n = int(client.scalar("SELECT count(*) FROM iceberg.marketing.campaigns"))

    return MaterializeResult(
        metadata={
            "rows_written": n,
            "written_to_main": True,
            "great_expectations": MetadataValue.json(summary),
        },
        check_results=[
            AssetCheckResult(
                check_name="great_expectations",
                passed=True,
                metadata={"checks": MetadataValue.json(summary)},
            )
        ],
    )

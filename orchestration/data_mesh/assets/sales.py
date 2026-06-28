"""Domínio VENDAS — asset `sales.orders` com quality gate (Great Expectations).

Padrão write-audit-publish: valida o DataFrame candidato; só escreve em
`main` se a suite passar. Falha => não escreve (qualidade no write).
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

from data_mesh.quality.expectations import orders_expectations, summarize, validate
from data_mesh.resources import TrinoResource
from data_mesh.schema import ORDERS_COLUMNS, ORDERS_TYPES, ensure_orders_table
from data_mesh.seeds import df_to_records, load_orders

ORDERS_KEY = AssetKey(["sales", "orders"])


@asset(
    key=ORDERS_KEY,
    group_name="sales_domain",
    description="Pedidos do domínio Vendas. Iceberg, particionado por day(order_ts).",
    check_specs=[AssetCheckSpec(name="great_expectations", asset=ORDERS_KEY, blocking=True)],
)
def sales_orders(trino: TrinoResource) -> MaterializeResult:
    client = trino.get_client()
    df = load_orders()

    # --- AUDIT: valida antes de publicar ---
    result = validate(df, "orders", orders_expectations())
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

    # --- PUBLISH: tabela ok -> reescreve dados em main ---
    ensure_orders_table(client)
    client.execute("DELETE FROM iceberg.sales.orders")
    records = df_to_records(df, ORDERS_COLUMNS)
    client.insert("iceberg.sales.orders", ORDERS_COLUMNS, ORDERS_TYPES, records)
    n = int(client.scalar("SELECT count(*) FROM iceberg.sales.orders"))

    return MaterializeResult(
        metadata={
            "rows_written": n,
            "written_to_main": True,
            "partitioning": "day(order_ts)",
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

"""Validações de qualidade com Great Expectations (API 1.x, contexto efêmero).

Estratégia: validar o DataFrame candidato ANTES de publicar (write-audit-publish).
Se falhar, o write não é promovido para `main`.
"""
from __future__ import annotations

from typing import Any

import great_expectations as gx
import pandas as pd
from great_expectations import expectations as gxe

from data_mesh.schema import VALID_COUNTRIES


def orders_expectations() -> list:
    """Suite de qualidade do domínio Vendas (orders)."""
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="order_id"),
        gxe.ExpectColumnValuesToBeUnique(column="order_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="campaign_id"),
        gxe.ExpectColumnValuesToBeBetween(column="amount", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="quantity", min_value=1, max_value=100),
        gxe.ExpectColumnValuesToBeInSet(column="country", value_set=VALID_COUNTRIES),
    ]


def campaigns_expectations() -> list:
    """Suite de qualidade do domínio Marketing (campaigns)."""
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="campaign_id"),
        gxe.ExpectColumnValuesToBeUnique(column="campaign_id"),
        gxe.ExpectColumnValuesToBeBetween(column="budget", min_value=0),
    ]


def validate(df: pd.DataFrame, asset_name: str, expectation_list: list):
    """Roda uma suite de expectations contra um DataFrame e devolve o resultado GE."""
    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas(name=f"{asset_name}_source")
    data_asset = data_source.add_dataframe_asset(name=asset_name)
    batch_def = data_asset.add_batch_definition_whole_dataframe("batch")

    suite = context.suites.add(gx.ExpectationSuite(name=f"{asset_name}_suite"))
    for exp in expectation_list:
        suite.add_expectation(exp)

    validation_def = context.validation_definitions.add(
        gx.ValidationDefinition(name=f"{asset_name}_vdef", data=batch_def, suite=suite)
    )
    return validation_def.run(batch_parameters={"dataframe": df})


def summarize(result: Any) -> dict:
    """Converte o resultado GE em um dict simples para metadata do Dagster."""
    summary = {"success": bool(result.success), "checks": []}
    for r in result.results:
        cfg = r.expectation_config
        details = {
            k: v
            for k, v in (r.result or {}).items()
            if k in ("unexpected_count", "element_count", "observed_value", "unexpected_percent")
        }
        summary["checks"].append(
            {
                "expectation": getattr(cfg, "type", str(cfg)),
                "column": (cfg.kwargs or {}).get("column"),
                "success": bool(r.success),
                "details": details,
            }
        )
    return summary

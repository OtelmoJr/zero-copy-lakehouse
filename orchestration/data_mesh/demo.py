"""DEMO DATA-AS-CODE — Write-Audit-Publish com branches do Nessie.

Roteiro:
  1) Lote com dados RUINS  -> escreve no branch `etl_branch` -> GE FALHA
     -> rollback (drop do branch). `main` nunca enxerga os dados ruins.
  2) Lote com dados BONS   -> escreve no branch `etl_branch` -> GE PASSA
     -> merge atômico em `main`. Consumidores veem tudo de uma vez.

Zero-copy: o branch compartilha os arquivos de `main` (copy-on-write).
Rode dentro do container:  docker compose exec dagster python -m data_mesh.demo
"""
from __future__ import annotations

import pandas as pd

from data_mesh.nessie import NessieClient
from data_mesh.quality.expectations import orders_expectations, summarize, validate
from data_mesh.schema import ORDERS_COLUMNS, ORDERS_TYPES, ensure_orders_table
from data_mesh.trino_io import TrinoClient

BRANCH = "etl_branch"
DEV_CATALOG = "iceberg_dev"  # mesmo data lake, ref=etl_branch
MAIN_TABLE = "iceberg.sales.orders"
DEV_TABLE = f"{DEV_CATALOG}.sales.orders"

# Lote BOM (passa em todas as expectations)
GOOD_BATCH = [
    ("ORD-90001", "2024-09-01 09:15:00", "CUST-0007", "CMP-003", "monitor", 1, 299.90, 299.90, "BR"),
    ("ORD-90002", "2024-09-01 10:05:00", "CUST-0012", "CMP-006", "dock", 2, 149.50, 299.00, "US"),
    ("ORD-90003", "2024-09-01 11:42:00", "CUST-0031", "CMP-003", "headset", 3, 79.90, 239.70, "PT"),
]

# Lote RUIM (amount negativo + campaign_id nulo -> GE deve falhar)
BAD_BATCH = [
    ("ORD-91001", "2024-09-02 08:00:00", "CUST-0004", "CMP-003", "mouse", 1, 31.90, -31.90, "BR"),
    ("ORD-91002", "2024-09-02 08:30:00", "CUST-0009", None, "keyboard", 2, 61.60, 123.20, "US"),
]


def _candidate_df(records: list) -> pd.DataFrame:
    return pd.DataFrame(records, columns=ORDERS_COLUMNS)


def _write_audit_publish(nessie: NessieClient, client: TrinoClient, records: list, label: str, log) -> bool:
    log(f"\n=== Lote '{label}': WRITE -> AUDIT -> PUBLISH/ROLLBACK ===")

    # WRITE: branch limpo a partir de main + insert no branch
    nessie.recreate_branch(BRANCH, "main")
    log(f"  [write] branch '{BRANCH}' criado a partir de main")
    ensure_orders_table(client, catalog=DEV_CATALOG)
    client.insert(DEV_TABLE, ORDERS_COLUMNS, ORDERS_TYPES, records, catalog=DEV_CATALOG)
    branch_count = int(client.scalar(f"SELECT count(*) FROM {DEV_TABLE}", catalog=DEV_CATALOG))
    main_count = int(client.scalar(f"SELECT count(*) FROM {MAIN_TABLE}"))
    log(f"  [write] orders no branch={branch_count} | em main={main_count} (isolado)")

    # AUDIT: Great Expectations no DataFrame candidato
    result = validate(_candidate_df(records), f"orders_{label}", orders_expectations())
    summary = summarize(result)
    log(f"  [audit] Great Expectations success={summary['success']}")
    for chk in summary["checks"]:
        if not chk["success"]:
            log(f"          FALHOU: {chk['expectation']} (col={chk['column']}) {chk['details']}")

    # PUBLISH ou ROLLBACK
    if summary["success"]:
        nessie.merge_branch(BRANCH, "main", message=f"publish lote {label}")
        nessie.delete_branch(BRANCH)
        log("  [publish] MERGE etl_branch -> main (atômico). Branch removido.")
        return True
    else:
        nessie.delete_branch(BRANCH)
        log("  [rollback] DROP BRANCH etl_branch. main intacto. Nada publicado.")
        return False


def run_demo(nessie: NessieClient, client: TrinoClient, log=print) -> dict:
    before = int(client.scalar(f"SELECT count(*) FROM {MAIN_TABLE}"))
    log(f"main.orders ANTES = {before}")

    bad_published = _write_audit_publish(nessie, client, BAD_BATCH, "RUIM", log)
    good_published = _write_audit_publish(nessie, client, GOOD_BATCH, "BOM", log)

    after = int(client.scalar(f"SELECT count(*) FROM {MAIN_TABLE}"))
    log(f"\nmain.orders DEPOIS = {after}  (delta = {after - before})")
    log("Resumo: lote RUIM rejeitado (rollback), lote BOM publicado (merge).")

    return {
        "main_before": before,
        "main_after": after,
        "bad_published": bad_published,
        "good_published": good_published,
    }


if __name__ == "__main__":
    run_demo(NessieClient.from_env(), TrinoClient.from_env())

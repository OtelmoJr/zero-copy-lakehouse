"""Job Dagster que executa a demo data-as-code (Write-Audit-Publish)."""
from __future__ import annotations

from dagster import OpExecutionContext, job, op

from data_mesh.demo import run_demo
from data_mesh.trino_io import TrinoClient


@op(required_resource_keys={"nessie"})
def data_as_code_op(context: OpExecutionContext) -> dict:
    nessie = context.resources.nessie.get_client()
    client = TrinoClient.from_env()
    return run_demo(nessie, client, log=context.log.info)


@job(
    description="Demo data-as-code: branch -> validar (GE) -> merge ou rollback no Nessie."
)
def data_as_code_job():
    data_as_code_op()

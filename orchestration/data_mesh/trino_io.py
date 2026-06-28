"""Camada fina de acesso ao Trino (sem dependência de Dagster).

Reutilizada por: resources do Dagster, assets e o demo data-as-code (CLI).
"""
from __future__ import annotations

import os
from typing import Any, Sequence

import trino


def _lit(value: Any, sql_type: str) -> str:
    """Converte um valor Python em literal SQL do Trino."""
    if value is None or (isinstance(value, float) and value != value):  # None ou NaN
        return "NULL"
    if sql_type == "str":
        return "'" + str(value).replace("'", "''") + "'"
    if sql_type == "int":
        return str(int(value))
    if sql_type == "double":
        return repr(float(value))
    if sql_type == "ts":
        return "TIMESTAMP '" + str(value) + "'"
    if sql_type == "date":
        return "DATE '" + str(value) + "'"
    raise ValueError(f"tipo SQL desconhecido: {sql_type}")


class TrinoClient:
    """Wrapper minimalista sobre trino.dbapi."""

    def __init__(self, host: str = "localhost", port: int = 8080, user: str = "dagster"):
        self.host = host
        self.port = port
        self.user = user

    @classmethod
    def from_env(cls) -> "TrinoClient":
        return cls(
            host=os.getenv("TRINO_HOST", "localhost"),
            port=int(os.getenv("TRINO_PORT", "8080")),
            user=os.getenv("TRINO_USER", "dagster"),
        )

    def _connect(self, catalog: str = "iceberg"):
        return trino.dbapi.connect(
            host=self.host, port=self.port, user=self.user, catalog=catalog
        )

    def execute(self, sql: str, catalog: str = "iceberg") -> tuple[list[str], list[tuple]]:
        """Executa um statement e devolve (colunas, linhas). fetchall() força a execução."""
        conn = self._connect(catalog)
        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description] if cur.description else []
            return cols, rows
        finally:
            cur.close()
            conn.close()

    def scalar(self, sql: str, catalog: str = "iceberg") -> Any:
        _, rows = self.execute(sql, catalog)
        return rows[0][0] if rows else None

    def insert(
        self,
        table: str,
        columns: Sequence[str],
        col_types: Sequence[str],
        records: Sequence[Sequence[Any]],
        catalog: str = "iceberg",
        chunk: int = 200,
    ) -> int:
        """INSERT em lotes a partir de uma lista de tuplas alinhadas a `columns`."""
        if not records:
            return 0
        conn = self._connect(catalog)
        cur = conn.cursor()
        collist = ", ".join(columns)
        try:
            for i in range(0, len(records), chunk):
                part = records[i : i + chunk]
                values = ", ".join(
                    "(" + ", ".join(_lit(v, col_types[j]) for j, v in enumerate(row)) + ")"
                    for row in part
                )
                cur.execute(f"INSERT INTO {table} ({collist}) VALUES {values}")
                cur.fetchall()
            return len(records)
        finally:
            cur.close()
            conn.close()

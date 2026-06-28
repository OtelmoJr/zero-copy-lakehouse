"""Resources do Dagster: front-door para Trino e Nessie."""
from __future__ import annotations

from dagster import ConfigurableResource

from data_mesh.nessie import NessieClient
from data_mesh.trino_io import TrinoClient


class TrinoResource(ConfigurableResource):
    host: str = "localhost"
    port: int = 8080
    user: str = "dagster"

    def get_client(self) -> TrinoClient:
        return TrinoClient(self.host, self.port, self.user)


class NessieResource(ConfigurableResource):
    uri: str = "http://localhost:19120/api/v2"

    def get_client(self) -> NessieClient:
        return NessieClient(self.uri)

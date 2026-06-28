"""Cliente Nessie REST v2 — operações git-like sobre o catálogo Iceberg.

create / merge / delete de branches = o coração do "data-as-code".
Docs: POST /trees, POST /trees/{branch}/history/merge, DELETE /trees/{ref}
"""
from __future__ import annotations

import os

import requests


class NessieClient:
    def __init__(self, base_url: str = "http://localhost:19120/api/v2", timeout: int = 30):
        self.base = base_url.rstrip("/")
        self.timeout = timeout

    @classmethod
    def from_env(cls) -> "NessieClient":
        return cls(os.getenv("NESSIE_URI", "http://localhost:19120/api/v2"))

    # ------------------------------------------------------------------ refs
    def get_reference(self, name: str) -> dict:
        """Lê uma referência (branch/tag) e seu hash HEAD."""
        r = requests.get(f"{self.base}/trees/{name}", timeout=self.timeout)
        r.raise_for_status()
        data = r.json()
        return data.get("reference", data)

    def list_references(self) -> list[dict]:
        r = requests.get(f"{self.base}/trees", timeout=self.timeout)
        r.raise_for_status()
        return r.json().get("references", [])

    def branch_exists(self, name: str) -> bool:
        r = requests.get(f"{self.base}/trees/{name}", timeout=self.timeout)
        return r.status_code == 200

    # -------------------------------------------------------------- mutations
    def create_branch(self, name: str, from_ref: str = "main") -> dict:
        src = self.get_reference(from_ref)
        body = {
            "type": "BRANCH",
            "name": name,
            "hash": src["hash"],
            "reference": {"type": "BRANCH", "name": from_ref, "hash": src["hash"]},
        }
        r = requests.post(f"{self.base}/trees", json=body, timeout=self.timeout)
        r.raise_for_status()
        data = r.json()
        return data.get("reference", data)

    def recreate_branch(self, name: str, from_ref: str = "main") -> dict:
        """Garante um branch limpo: dropa se existir, recria a partir de from_ref."""
        if self.branch_exists(name):
            self.delete_branch(name)
        return self.create_branch(name, from_ref)

    def merge_branch(self, from_branch: str, into: str = "main", message: str | None = None) -> dict:
        """Publica (merge) os commits de from_branch em `into` de forma atômica."""
        src = self.get_reference(from_branch)
        body = {"fromRefName": from_branch, "fromHash": src["hash"]}
        if message:
            body["message"] = message
        r = requests.post(
            f"{self.base}/trees/{into}/history/merge", json=body, timeout=self.timeout
        )
        r.raise_for_status()
        return r.json()

    def delete_branch(self, name: str) -> bool:
        """Rollback: descarta o branch inteiro (todos os commits não mergeados)."""
        ref = self.get_reference(name)
        r = requests.delete(
            f"{self.base}/trees/{name}@{ref['hash']}", timeout=self.timeout
        )
        r.raise_for_status()
        return True

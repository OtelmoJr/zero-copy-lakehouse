"""Definitions do Dagster — junta assets, checks, job e resources."""
from __future__ import annotations

import os

from dagster import Definitions

from data_mesh.assets.analytics import revenue_by_campaign
from data_mesh.assets.marketing import marketing_campaigns
from data_mesh.assets.sales import sales_orders
from data_mesh.jobs import data_as_code_job
from data_mesh.resources import NessieResource, TrinoResource

defs = Definitions(
    assets=[sales_orders, marketing_campaigns, revenue_by_campaign],
    jobs=[data_as_code_job],
    resources={
        "trino": TrinoResource(
            host=os.getenv("TRINO_HOST", "localhost"),
            port=int(os.getenv("TRINO_PORT", "8080")),
            user=os.getenv("TRINO_USER", "dagster"),
        ),
        "nessie": NessieResource(uri=os.getenv("NESSIE_URI", "http://localhost:19120/api/v2")),
    },
)

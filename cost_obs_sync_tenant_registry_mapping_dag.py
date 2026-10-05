"""Cost Observability: sync the Tenant Registry mapping to ClickHouse.

Keeps ClickHouse ``tenant_registry_mapping`` in sync with Tenant Registry, one
``(instance_id, in_app_id, tenant_id)`` row per tenant on an Express instance. Tasks:

- ``fetch_tenant_mapping``: lists Express instances from Tenant Registry
  (``GET /api/v0/applications/id/com.medallia.express/instances/``), reduced to one row per tenant.
- ``truncate_staging_table``: empties ``tenant_registry_mapping_staging``.
- ``load_staging_table``: bulk inserts the mapping into the staging copy. Fails instead of
  loading if Tenant Registry returned no tenants, so an outage can't wipe the table.
- ``swap_tables``: ``EXCHANGE TABLES`` swaps the staging copy with the real table atomically,
  so readers never see a partial or empty table, and tenants removed from the registry disappear.

Both ``tenant_registry_mapping`` and ``tenant_registry_mapping_staging`` must already exist in
ClickHouse with the same schema; the DAG doesn't create them.

Connections: ``tenant-registry`` (HTTP, host ``https://tenant-registry.eng.medallia.com``, no auth)
and ``sharedservices-clickhouse-spf-test`` (ClickHouse).
"""

import logging
from datetime import datetime
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.operators.http import HttpOperator

log = logging.getLogger(__name__)

TENANT_REGISTRY_CONN_ID = "tenant-registry"
TENANT_REGISTRY_ENDPOINT = "/api/v0/applications/id/com.medallia.express/instances/"
CLICKHOUSE_CONN_ID = "sharedservices-clickhouse-spf-test"
CLICKHOUSE_CLUSTER_NAME = "my_cluster"
CLICKHOUSE_TABLE = "tenant_registry_mapping"
CLICKHOUSE_STAGING_TABLE = f"{CLICKHOUSE_TABLE}_staging"
CLICKHOUSE_COLUMNS = ["instance_id", "in_app_id", "tenant_id"]


def parse_tenant_mapping(instances_response: dict) -> List[Dict[str, Any]]:
    """Map each tenant to its instance from a Tenant Registry Express instances response.

    The top-level ``tenant_id`` of an item is the instance id; the item's ``tenants`` list
    holds the actual tenants, each with its own ``tenant_id`` and ``in_app_id``.
    Returns one row per tenant: ``{"instance_id", "in_app_id", "tenant_id"}``.
    """
    instances = instances_response.get("items", [])
    if instances_response.get("_total", len(instances)) != len(instances):
        log.warning("Tenant Registry returned %d of %d instances", len(instances), instances_response["_total"])

    tenant_mapping = [
        {
            "instance_id": instance["tenant_id"],
            "in_app_id": tenant.get("in_app_id"),
            "tenant_id": tenant["tenant_id"],
        }
        for instance in instances
        for tenant in instance.get("tenants", [])
    ]
    log.info(
        "Mapped %d tenants across %d Express instances",
        len(tenant_mapping),
        len({row["instance_id"] for row in tenant_mapping}),
    )
    return tenant_mapping


def build_clickhouse_rows(tenant_mapping: List[Dict[str, Any]]) -> List[tuple]:
    """Turn ``parse_tenant_mapping`` output into ``tenant_registry_mapping`` rows, in ``CLICKHOUSE_COLUMNS`` order."""
    return [tuple(row[column] for column in CLICKHOUSE_COLUMNS) for row in tenant_mapping]


@dag(
    dag_display_name="Cost Observability: Sync Tenant Registry Mapping",
    tags=["spf", "clickhouse", "cost-observability"],
    description="Sync Tenant Registry (instance_id, in_app_id, tenant_id) to ClickHouse tenant_registry_mapping.",
    doc_md=__doc__,
    max_active_runs=1,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    render_template_as_native_obj=True,
)
def cost_obs_sync_tenant_registry_mapping_dag():
    fetch_tenant_mapping = HttpOperator(
        task_id="fetch_tenant_mapping",
        http_conn_id=TENANT_REGISTRY_CONN_ID,
        method="GET",
        endpoint=TENANT_REGISTRY_ENDPOINT,
        headers={"Accept": "application/json"},
        response_filter=lambda response: parse_tenant_mapping(response.json()),
    )

    truncate_staging_table = SQLExecuteQueryOperator(
        task_id="truncate_staging_table",
        conn_id=CLICKHOUSE_CONN_ID,
        sql=f"TRUNCATE TABLE {CLICKHOUSE_STAGING_TABLE}",
    )

    @task
    def load_staging_table(tenant_mapping: List[Dict[str, Any]]) -> int:
        rows = build_clickhouse_rows(tenant_mapping)
        if not rows:
            raise ValueError("Tenant Registry returned no tenants; refusing to replace the mapping with an empty table")
        ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID).bulk_insert_rows(
            CLICKHOUSE_STAGING_TABLE, rows, column_names=CLICKHOUSE_COLUMNS
        )
        return len(rows)

    swap_tables = SQLExecuteQueryOperator(
        task_id="swap_tables",
        conn_id=CLICKHOUSE_CONN_ID,
        sql=f"EXCHANGE TABLES {CLICKHOUSE_STAGING_TABLE} AND {CLICKHOUSE_TABLE} ON CLUSTER {CLICKHOUSE_CLUSTER_NAME}",
    )

    truncate_staging_table >> load_staging_table(fetch_tenant_mapping.output) >> swap_tables


cost_obs_sync_tenant_registry_mapping_dag()

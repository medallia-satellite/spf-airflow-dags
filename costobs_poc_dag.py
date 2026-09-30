"""Cost Observability POC.

Collects per-index CPU, memory, and storage figures from Elasticsearch ``GET /_stats``
fetches the tenant mapping from ClickHouse, and pulls the instance list from Tenant Registry.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
from airflow.providers.http.operators.http import HttpOperator

log = logging.getLogger(__name__)

ES_CONN_ID = "sharedservices-elasticsearch"
CH_CONN_ID = "sharedservices-clickhouse-spf-test"
TENANT_REGISTRY_CONN_ID = "tenant-registry"
EXPRESS_APPLICATION_ID = "com.medallia.express"

CPU_FIELDS = [
    ("search", "query_time_in_millis"),
    ("search", "fetch_time_in_millis"),
    ("indexing", "index_time_in_millis"),
]
MEMORY_FIELDS = [
    ("segments", "memory_in_bytes"),
    ("segments", "index_writer_memory_in_bytes"),
    ("segments", "version_map_memory_in_bytes"),
    ("segments", "fixed_bit_set_memory_in_bytes"),
    ("fielddata", "memory_size_in_bytes"),
    ("query_cache", "memory_size_in_bytes"),
    ("request_cache", "memory_size_in_bytes"),
]
STORAGE_FIELDS = [
    ("store", "size_in_bytes"),
    ("translog", "size_in_bytes"),
]


def summarize(stats_response: dict) -> Dict[str, Dict[str, int]]:
    """Reduce a ``GET /_stats`` response to cpu_ms, memory_bytes and storage_bytes per index.

    Uses the ``total`` section, so primaries and replicas are both counted.
    cpu_ms is the time spent on search and indexing, added up since the shards started.
    It is a running total, not the time spent since the last DAG run.
    """

    def total(stats: dict, fields) -> int:
        return sum(stats.get(section, {}).get(field, 0) for section, field in fields)

    out = {}
    for index, s in stats_response.get("indices", {}).items():
        t = s.get("total", {})
        out[index] = {
            "cpu_ms": total(t, CPU_FIELDS),
            "memory_bytes": total(t, MEMORY_FIELDS),
            "storage_bytes": total(t, STORAGE_FIELDS),
        }
    return out


def parse_tenant_mapping(response: dict) -> List[Dict[str, Any]]:
    """Map each tenant to its instance from a Tenant Registry ``GET /api/v0/instances`` response.

    The top-level ``tenant_id`` of an item is the instance id; the item's ``tenants`` list
    holds the actual tenants, each with its own ``tenant_id`` and ``in_app_id``.
    Only Express instances are included.
    Returns one row per tenant: ``{"instance_id", "in_app_id", "tenant_id"}``.
    """
    items = response.get("items", [])
    if response.get("_total", len(items)) != len(items):
        log.warning("Tenant Registry returned %d of %d instances", len(items), response["_total"])

    mapping = [
        {
            "instance_id": instance["tenant_id"],
            "in_app_id": tenant.get("in_app_id"),
            "tenant_id": tenant["tenant_id"],
        }
        for instance in items
        if instance.get("application_id") == EXPRESS_APPLICATION_ID
        for tenant in instance.get("tenants", [])
    ]
    log.info(
        "Mapped %d tenants across %d Express instances",
        len(mapping),
        len({m["instance_id"] for m in mapping}),
    )
    return mapping


@dag(
    dag_display_name="Cost Observability POC",
    tags=["spf", "elasticsearch", "clickhouse", "cost-observability"],
    description="POC: collect per-index CPU/memory/storage from Elasticsearch and read ClickHouse index stats.",
    max_active_runs=1,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    render_template_as_native_obj=True,
)
def costobs_poc_dag():
    @task
    def collect_es_index_stats() -> Dict[str, Dict[str, int]]:
        conn = BaseHook.get_connection(ES_CONN_ID)
        host = f"{conn.schema}://{conn.host}" + (f":{conn.port}" if conn.port else "")
        es = ElasticsearchPythonHook(hosts=[host], es_conn_args=conn.extra_dejson).get_conn

        stats = es.indices.stats(index="_all", filter_path="indices.*.total")
        summary = summarize(stats)

        log.info(
            "Collected stats for %d indices: cpu_ms=%d memory_bytes=%d storage_bytes=%d",
            len(summary),
            sum(m["cpu_ms"] for m in summary.values()),
            sum(m["memory_bytes"] for m in summary.values()),
            sum(m["storage_bytes"] for m in summary.values()),
        )
        return summary

    fetch_tenant_registry_instances = HttpOperator(
        task_id="fetch_tenant_registry_instances",
        http_conn_id=TENANT_REGISTRY_CONN_ID,
        method="GET",
        endpoint="/api/v0/instances",
        headers={"Accept": "application/json"},
        response_filter=lambda response: parse_tenant_mapping(response.json()),
    )

    read_rows = SQLExecuteQueryOperator(
        task_id="read_rows",
        conn_id=CH_CONN_ID,
        sql="SELECT * FROM es_index_stats_hourly",
        handler=fetch_all_handler,
    )

    collect_es_index_stats() >> read_rows >> fetch_tenant_registry_instances


costobs_poc_dag()

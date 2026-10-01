"""Cost Observability POC.

Collects per-index CPU, memory, and storage figures from Elasticsearch ``GET /_stats``
fetches the tenant mapping from ClickHouse, and pulls the instance list from Tenant Registry.
"""

import logging
import re
from datetime import datetime
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.operators.http import HttpOperator

log = logging.getLogger(__name__)

ES_CONN_ID = "sharedservices-elasticsearch"
CH_CONN_ID = "sharedservices-clickhouse-spf-test"
TENANT_REGISTRY_CONN_ID = "tenant-registry"
EXPRESS_APPLICATION_ID = "com.medallia.express"

# seaas-<in_app_id>_topic-builder-...-<in_app_id>[-YYYY-MM-DD]-<instance_id>-<suffix>
# Same shape as fix_and_verify.INDEX_PATTERN, but the month is optional.
INDEX_REGEX = re.compile(
    r"^seaas-(?P<in_app_id>\w+)_topic-builder(-\w+)+(\.\w{2,4}){0,2}(\.\w+)(\.\w{2,4}){1,2}-(?P=in_app_id)"
    r"(-(?P<month>[0-9]{4}-[0-9]{2}-[0-9]{2}))?-(?P<instance_id>[0-9]+)-(?P<suffix>[0-9]+)$"
)

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

    log.info(
        "Collected stats for %d indices: cpu_ms=%d memory_bytes=%d storage_bytes=%d",
        len(out),
        sum(m["cpu_ms"] for m in out.values()),
        sum(m["memory_bytes"] for m in out.values()),
        sum(m["storage_bytes"] for m in out.values()),
    )
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


def map_indices(
    index_stats: Dict[str, Dict[str, int]], tenant_mapping: List[Dict[str, Any]]
) -> Dict[str, Dict[str, Any]]:
    """Attach ``instance_id``, ``in_app_id`` and ``tenant_id`` to each index's stats.

    Indices that don't match ``INDEX_REGEX`` or aren't in the tenant mapping are left out and counted in the log.
    """
    tenant_by_instance_app = {(r["instance_id"], r["in_app_id"]): r["tenant_id"] for r in tenant_mapping}

    out = {}
    unparsed = unmapped = 0
    for index, stats in index_stats.items():
        match = INDEX_REGEX.fullmatch(index)
        if not match:
            unparsed += 1
            continue
        instance_id, in_app_id = int(match["instance_id"]), match["in_app_id"]
        tenant_id = tenant_by_instance_app.get((instance_id, in_app_id))
        if tenant_id is None:
            unmapped += 1
            continue
        out[index] = {**stats, "instance_id": instance_id, "in_app_id": in_app_id, "tenant_id": tenant_id}

    log.info(
        "Mapped %d of %d indices to tenants (%d unparsed, %d not in Tenant Registry)",
        len(out),
        len(index_stats),
        unparsed,
        unmapped,
    )
    return out


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
    collect_es_index_stats = HttpOperator(
        task_id="collect_es_index_stats",
        http_conn_id=ES_CONN_ID,
        method="GET",
        endpoint="/_all/_stats",
        data={"filter_path": "indices.*.total"},
        headers={"Accept": "application/json"},
        response_filter=lambda response: summarize(response.json()),
    )

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

    @task
    def map_indices_to_tenants(
        index_stats: Dict[str, Dict[str, int]], tenant_mapping: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        return map_indices(index_stats, tenant_mapping)

    collect_es_index_stats >> read_rows >> fetch_tenant_registry_instances
    map_indices_to_tenants(collect_es_index_stats.output, fetch_tenant_registry_instances.output)


costobs_poc_dag()

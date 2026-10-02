"""Cost Observability POC.

Attributes Elasticsearch resource usage to tenants. Tasks:

- ``collect_es_index_stats``: ``GET /_all/_stats`` on Elasticsearch, reduced to
  ``docs_count``, ``cpu_ms``, ``memory_bytes`` and ``storage_bytes`` per index.
- ``read_es_index_stats_hourly``: reads the ``es_index_stats_hourly`` table from ClickHouse.
- ``fetch_tenant_mapping``: lists Express instances from Tenant Registry
  (``GET /api/v0/applications/id/com.medallia.express/instances/``), reduced to one
  ``{instance_id, in_app_id, tenant_id}`` row per tenant.
- ``map_indices_to_tenants``: parses ``instance_id`` and ``in_app_id`` from each index name
  and looks up the ``tenant_id``.
- ``insert_es_index_stats_hourly``: writes one row per index to ClickHouse
  ``es_index_stats_hourly``, with ``ts`` set to the run's ``data_interval_end``.
  Indices without a tenant are written with ``tenant_id = 0``.

Params ``dc`` and ``namespace`` describe where the ``sharedservices-elasticsearch`` cluster runs
and are written to the matching columns.

Connections: ``sharedservices-elasticsearch``, ``sharedservices-clickhouse-spf-test`` (ClickHouse)
and ``tenant-registry`` (HTTP, host ``https://tenant-registry.eng.medallia.com``, no auth).
"""

import logging
import re
from datetime import datetime
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.models import Param
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook
from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.operators.http import HttpOperator

log = logging.getLogger(__name__)

ELASTICSEARCH_CONN_ID = "sharedservices-elasticsearch"
CLICKHOUSE_CONN_ID = "sharedservices-clickhouse-spf-test"
CLICKHOUSE_TABLE = "elasticsearch_index_stats_hourly"
CLICKHOUSE_COLUMNS = [
    "ts",
    "dc",
    "namespace",
    "index_name",
    "total_docs_count",
    "cpu_ms_cumulative",
    "total_mem_bytes",
    "total_store_bytes",
    "tenant_id",
]
# tenant_id is part of the table's primary key, so it can't be NULL; 0 marks unattributed indices.
UNATTRIBUTED_TENANT_ID = 0
TENANT_REGISTRY_CONN_ID = "tenant-registry"
TENANT_REGISTRY_ENDPOINT = "/api/v0/applications/id/com.medallia.express/instances/"

# seaas-<in_app_id>_surveys-...-<in_app_id>-<instance_id>-<suffix>
SEAAS_INDEX_REGEX = re.compile(
    r"^seaas-(?P<in_app_id>\w+)_surveys(-\w+)+(\.\w{2,4}){0,2}(\.\w+)(\.\w{2,4}){1,2}-(?P=in_app_id)"
    r"-(?P<instance_id>[0-9]+)-(?P<suffix>[0-9]+)$"
)

# (section, field) pairs from the ``total`` block of ``GET /_stats``, added up per metric.
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


def summarize_index_stats(stats_response: dict) -> Dict[str, Dict[str, int]]:
    """Reduce a ``GET /_stats`` response to docs_count, cpu_ms, memory_bytes and storage_bytes per index.

    Uses the ``total`` section, so primaries and replicas are both counted.
    cpu_ms is the time spent on search and indexing, added up since the shards started.
    It is a running total, not the time spent since the last DAG run.
    """

    def sum_fields(totals: dict, fields) -> int:
        return sum(totals.get(section, {}).get(field, 0) for section, field in fields)

    index_stats = {}
    for index, stats in stats_response.get("indices", {}).items():
        totals = stats.get("total", {})
        index_stats[index] = {
            "docs_count": totals.get("docs", {}).get("count", 0),
            "cpu_ms": sum_fields(totals, CPU_FIELDS),
            "memory_bytes": sum_fields(totals, MEMORY_FIELDS),
            "storage_bytes": sum_fields(totals, STORAGE_FIELDS),
        }

    log.info(
        "Collected stats for %d indices: cpu_ms=%d memory_bytes=%d storage_bytes=%d",
        len(index_stats),
        sum(s["cpu_ms"] for s in index_stats.values()),
        sum(s["memory_bytes"] for s in index_stats.values()),
        sum(s["storage_bytes"] for s in index_stats.values()),
    )
    return index_stats


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


def attach_tenants(
    index_stats: Dict[str, Dict[str, int]], tenant_mapping: List[Dict[str, Any]]
) -> Dict[str, Dict[str, Any]]:
    """Add ``instance_id``, ``in_app_id`` and ``tenant_id`` to each index's stats.

    ``instance_id`` and ``in_app_id`` are parsed from the index name with ``SEAAS_INDEX_REGEX``;
    ``tenant_id`` is looked up in ``tenant_mapping``. Every index is kept: fields that can't be
    determined are None. A name that doesn't match leaves all three None; a match that isn't in
    the mapping keeps the parsed ``instance_id`` and ``in_app_id`` with ``tenant_id`` None.
    Each unmapped index gets a warning, except non-``seaas-`` indices (system indices like
    ``.kibana``), which are only counted.
    """
    tenant_id_by_instance_app = {
        (row["instance_id"], row["in_app_id"]): row["tenant_id"] for row in tenant_mapping
    }

    tenant_index_stats = {}
    mapped_count = non_seaas_count = unparsed_count = unmapped_count = 0
    for index, stats in index_stats.items():
        instance_id = in_app_id = tenant_id = None
        match = SEAAS_INDEX_REGEX.fullmatch(index)
        if not index.startswith("seaas-"):
            non_seaas_count += 1
        elif not match:
            log.warning("Cannot map %s: name doesn't match the seaas surveys index pattern", index)
            unparsed_count += 1
        else:
            instance_id, in_app_id = int(match["instance_id"]), match["in_app_id"]
            tenant_id = tenant_id_by_instance_app.get((instance_id, in_app_id))
            if tenant_id is None:
                log.warning(
                    "Cannot map %s: instance_id=%d in_app_id=%s not found in Tenant Registry",
                    index,
                    instance_id,
                    in_app_id,
                )
                unmapped_count += 1
            else:
                mapped_count += 1

        tenant_index_stats[index] = {
            **stats,
            "instance_id": instance_id,
            "in_app_id": in_app_id,
            "tenant_id": tenant_id,
        }

    log.info(
        "Mapped %d of %d indices to tenants (%d non-seaas, %d unparsed, %d not in Tenant Registry)",
        mapped_count,
        len(index_stats),
        non_seaas_count,
        unparsed_count,
        unmapped_count,
    )
    return tenant_index_stats


def build_clickhouse_rows(
    tenant_index_stats: Dict[str, Dict[str, Any]], ts: datetime, dc: str, namespace: str
) -> List[tuple]:
    """Turn ``attach_tenants`` output into ``es_index_stats_hourly`` rows, in ``CLICKHOUSE_COLUMNS`` order."""
    return [
        (
            ts,
            dc,
            namespace,
            index,
            stats["docs_count"],
            stats["cpu_ms"],
            stats["memory_bytes"],
            stats["storage_bytes"],
            stats["tenant_id"] if stats["tenant_id"] is not None else UNATTRIBUTED_TENANT_ID,
        )
        for index, stats in tenant_index_stats.items()
    ]


@dag(
    dag_display_name="Cost Observability POC",
    tags=["spf", "elasticsearch", "clickhouse", "cost-observability"],
    description="POC: attribute Elasticsearch per-index CPU/memory/storage to tenants via Tenant Registry.",
    doc_md=__doc__,
    max_active_runs=1,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    render_template_as_native_obj=True,
    params={
        # Where ELASTICSEARCH_CONN_ID runs.
        "dc": Param("<dc>", type="string"),
        "namespace": Param("<namespace>", type="string"),
    },
)
def costobs_poc_dag():
    @task
    def map_indices_to_tenants(
        index_stats: Dict[str, Dict[str, int]], tenant_mapping: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        return attach_tenants(index_stats, tenant_mapping)

    collect_es_index_stats = HttpOperator(
        task_id="collect_es_index_stats",
        http_conn_id=ELASTICSEARCH_CONN_ID,
        method="GET",
        endpoint="/_all/_stats",
        data={"filter_path": "indices.*.total"},
        headers={"Accept": "application/json"},
        response_filter=lambda response: summarize_index_stats(response.json()),
    )

    @task
    def insert_es_index_stats_hourly(
        tenant_index_stats: Dict[str, Dict[str, Any]], data_interval_end=None, params=None
    ) -> int:
        rows = build_clickhouse_rows(tenant_index_stats, data_interval_end, params["dc"], params["namespace"])
        ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID).bulk_insert_rows(CLICKHOUSE_TABLE, rows, column_names=CLICKHOUSE_COLUMNS)
        log.info("Wrote ts=%s dc=%s namespace=%s", data_interval_end, params["dc"], params["namespace"])
        return len(rows)

    fetch_tenant_mapping = HttpOperator(
        task_id="fetch_tenant_mapping",
        http_conn_id=TENANT_REGISTRY_CONN_ID,
        method="GET",
        endpoint=TENANT_REGISTRY_ENDPOINT,
        headers={"Accept": "application/json"},
        response_filter=lambda response: parse_tenant_mapping(response.json()),
    )

    read_es_index_stats_hourly = SQLExecuteQueryOperator(
        task_id="read_es_index_stats_hourly",
        conn_id=CLICKHOUSE_CONN_ID,
        sql=f"SELECT * FROM {CLICKHOUSE_TABLE}",
        handler=fetch_all_handler,
    )

    mapped_index_stats = map_indices_to_tenants(collect_es_index_stats.output, fetch_tenant_mapping.output)
    insert_es_index_stats_hourly(mapped_index_stats)


costobs_poc_dag()

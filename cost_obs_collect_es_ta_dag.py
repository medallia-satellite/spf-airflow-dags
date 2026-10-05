"""Cost Observability: collect Elasticsearch index stats.

Collects per-index resource usage from Elasticsearch and stores it in ClickHouse. Tasks:

- ``collect_elasticsearch_index_stats``: ``GET /_all/_stats`` on Elasticsearch, reduced to
  ``docs_count``, ``cpu_ms``, ``memory_bytes`` and ``storage_bytes`` per seaas surveys index,
  plus the ``instance_id`` and ``in_app_id`` parsed from the index name.
- ``insert_elasticsearch_index_stats_raw``: writes one row per index to ClickHouse
  ``elasticsearch_index_stats_raw``, with ``ts`` set to the run's ``data_interval_end``.

Params ``dc`` and ``namespace`` describe where the ``sharedservices-elasticsearch`` cluster runs
and are written to the matching columns.

Connections: ``sharedservices-elasticsearch`` and ``sharedservices-clickhouse-spf-test`` (ClickHouse).
"""

import logging
import re
from datetime import datetime
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.models import Param
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook
from airflow.providers.http.operators.http import HttpOperator

log = logging.getLogger(__name__)

ELASTICSEARCH_CONN_ID = "sharedservices-elasticsearch"
CLICKHOUSE_CONN_ID = "sharedservices-clickhouse-spf-test"
CLICKHOUSE_TABLE = "elasticsearch_index_stats_raw"
CLICKHOUSE_COLUMNS = [
    "ts",
    "dc",
    "namespace",
    "index_name",
    "total_docs_count",
    "cpu_ms_cumulative",
    "total_mem_bytes",
    "total_store_bytes",
    "instance_id",
    "in_app_id",
]

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


def parse_index_stats(stats_response: dict) -> Dict[str, Dict[str, Any]]:
    """Reduce a ``GET /_stats`` response to per-index stats for seaas surveys indices.

    Each kept index gets docs_count, cpu_ms, memory_bytes and storage_bytes, plus the
    ``instance_id`` and ``in_app_id`` parsed from its name with ``SEAAS_INDEX_REGEX``.
    Non-``seaas-`` indices (system indices like ``.kibana``) are skipped silently;
    ``seaas-`` indices that don't match the pattern are skipped with a warning.

    Uses the ``total`` section, so primaries and replicas are both counted.
    cpu_ms is the time spent on search and indexing, added up since the shards started.
    It is a running total, not the time spent since the last DAG run.
    """

    def sum_fields(totals: dict, fields) -> int:
        return sum(totals.get(section, {}).get(field, 0) for section, field in fields)

    index_stats = {}
    for index, stats in stats_response.get("indices", {}).items():
        match = SEAAS_INDEX_REGEX.fullmatch(index)
        if not index.startswith("seaas-"):
            continue
        elif not match:
            log.warning("Skipping %s: name doesn't match the seaas surveys index pattern", index)
            continue

        instance_id, in_app_id = int(match["instance_id"]), match["in_app_id"]

        totals = stats.get("total", {})
        index_stats[index] = {
            "docs_count": totals.get("docs", {}).get("count", 0),
            "cpu_ms": sum_fields(totals, CPU_FIELDS),
            "memory_bytes": sum_fields(totals, MEMORY_FIELDS),
            "storage_bytes": sum_fields(totals, STORAGE_FIELDS),
            "instance_id": instance_id,
            "in_app_id": in_app_id,
        }

    log.info(
        "Collected stats for %d indices: cpu_ms=%d memory_bytes=%d storage_bytes=%d",
        len(index_stats),
        sum(s["cpu_ms"] for s in index_stats.values()),
        sum(s["memory_bytes"] for s in index_stats.values()),
        sum(s["storage_bytes"] for s in index_stats.values()),
    )

    return index_stats


def build_clickhouse_rows(
    index_stats: Dict[str, Dict[str, Any]], ts: datetime, dc: str, namespace: str
) -> List[tuple]:
    """Turn ``parse_index_stats`` output into ``elasticsearch_index_stats_raw`` rows, in ``CLICKHOUSE_COLUMNS`` order."""
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
            stats["instance_id"],
            stats["in_app_id"],
        )
        for index, stats in index_stats.items()
    ]


@dag(
    dag_display_name="Cost Observability: Collect Elasticsearch Index Stats",
    tags=["spf", "elasticsearch", "clickhouse", "cost-observability"],
    description=(
        "Collect per-index docs/CPU/memory/storage from Elasticsearch into ClickHouse "
        "elasticsearch_index_stats_raw."
    ),
    doc_md=__doc__,
    max_active_runs=1,
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    render_template_as_native_obj=True,
    params={
        # Where ELASTICSEARCH_CONN_ID runs.
        "dc": Param("den", type="string"),
        "namespace": Param("sharedservices-elasticsearch", type="string"),
    },
)
def cost_obs_collect_es_ta_dag():
    collect_elasticsearch_index_stats = HttpOperator(
        task_id="collect_elasticsearch_index_stats",
        http_conn_id=ELASTICSEARCH_CONN_ID,
        method="GET",
        endpoint="/_all/_stats",
        data={"filter_path": "indices.*.total"},
        headers={"Accept": "application/json"},
        response_filter=lambda response: parse_index_stats(response.json()),
    )

    @task
    def insert_elasticsearch_index_stats_raw(
        index_stats: Dict[str, Dict[str, Any]], data_interval_end=None, params=None
    ) -> int:
        rows = build_clickhouse_rows(index_stats, data_interval_end, params["dc"], params["namespace"])
        ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID).bulk_insert_rows(
            CLICKHOUSE_TABLE, rows, column_names=CLICKHOUSE_COLUMNS
        )
        log.info("Wrote ts=%s dc=%s namespace=%s", data_interval_end, params["dc"], params["namespace"])
        return len(rows)

    insert_elasticsearch_index_stats_raw(collect_elasticsearch_index_stats.output)


cost_obs_collect_es_ta_dag()

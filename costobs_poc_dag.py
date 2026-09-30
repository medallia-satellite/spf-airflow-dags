"""Cost Observability POC.

Collects per-index CPU, memory, and storage figures from Elasticsearch ``GET /_stats``
and fetches the tenant mapping from ClickHouse.
"""

import logging
from datetime import datetime
from typing import Dict

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook

log = logging.getLogger(__name__)

ES_CONN_ID = "sharedservices-elasticsearch"
CH_CONN_ID = "sharedservices-clickhouse-spf-test"

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

    read_rows = SQLExecuteQueryOperator(
        task_id="read_rows",
        conn_id=CH_CONN_ID,
        sql="SELECT * FROM tenant_mapping",
        handler=fetch_all_handler,
    )

    collect_es_index_stats() >> read_rows


costobs_poc_dag()

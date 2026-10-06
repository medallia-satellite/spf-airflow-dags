"""Cost Observability: collect Elasticsearch index stats.

Collects per-index resource usage from every configured Elasticsearch cluster and stores it in
ClickHouse. Tasks:

- ``load_elasticsearch_clusters``: reads the clusters to collect from (see below) and fails
  early if the configuration is invalid.
- ``collect_elasticsearch_index_stats`` (one mapped instance per cluster): ``GET /_all/_stats``
  on the cluster, reduced to ``docs_count``, ``cpu_ms``, ``memory_bytes`` and ``storage_bytes``
  per seaas surveys index, plus the ``instance_id`` and ``in_app_id`` parsed from the index name.
- ``insert_elasticsearch_index_stats_raw`` (one mapped instance per cluster): writes one row per
  index to ClickHouse ``elasticsearch_index_stats_raw``, with the cluster's ``dc`` / ``namespace``
  and ``ts`` set to the run's ``data_interval_end``.

Clusters are configured per Airflow deployment (this DAG runs in several DCs) in the JSON
Variable ``cost_obs_elasticsearch_clusters``. Each entry ties together the Elasticsearch
connection to collect from and the ``dc`` / ``namespace`` written to ClickHouse, so they can't
be mismatched::

    [
        {
            "elasticsearch_conn_id": "sharedservices-elasticsearch",
            "dc": "den",
            "namespace": "sharedservices-elasticsearch"
        }
    ]

Connections: each cluster's ``elasticsearch_conn_id`` and ``sharedservices-clickhouse-spf-test``
(ClickHouse).
"""

import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Tuple

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook
from airflow.providers.http.operators.http import HttpOperator

log = logging.getLogger(__name__)

# Per-deployment JSON Variable with this DC's clusters; format in the module docstring.
ELASTICSEARCH_CLUSTERS_VARIABLE = "cost_obs_elasticsearch_clusters"
ELASTICSEARCH_CLUSTER_FIELDS = ("elasticsearch_conn_id", "dc", "namespace")
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


def validate_clusters(clusters: Any) -> List[Dict[str, str]]:
    """Check the ``ELASTICSEARCH_CLUSTERS_VARIABLE`` value and return its clusters.

    Raises ``ValueError`` unless it's a non-empty list whose entries all have
    ``ELASTICSEARCH_CLUSTER_FIELDS``. Extra keys are dropped.
    """
    if not isinstance(clusters, list) or not clusters:
        raise ValueError(f"Variable {ELASTICSEARCH_CLUSTERS_VARIABLE} must be a non-empty JSON list of clusters")
    for i, cluster in enumerate(clusters):
        missing = [field for field in ELASTICSEARCH_CLUSTER_FIELDS if field not in cluster]
        if missing:
            raise ValueError(f"Variable {ELASTICSEARCH_CLUSTERS_VARIABLE} entry {i} is missing {missing}")
    log.info("Collecting from %d Elasticsearch clusters: %s", len(clusters), clusters)
    return [{field: cluster[field] for field in ELASTICSEARCH_CLUSTER_FIELDS} for cluster in clusters]


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
)
def cost_obs_collect_elasticsearch_ta_dag():
    @task
    def load_elasticsearch_clusters() -> List[Dict[str, str]]:
        return validate_clusters(Variable.get(ELASTICSEARCH_CLUSTERS_VARIABLE, deserialize_json=True))

    clusters = load_elasticsearch_clusters()

    # One mapped instance per cluster, labelled with its connection id in the UI.
    collect_elasticsearch_index_stats = HttpOperator.partial(
        task_id="collect_elasticsearch_index_stats",
        map_index_template="{{ task.http_conn_id }}",
        method="GET",
        endpoint="/_all/_stats",
        data={"filter_path": "indices.*.total"},
        headers={"Accept": "application/json"},
        response_filter=lambda response: parse_index_stats(response.json()),
    ).expand(http_conn_id=clusters.map(lambda cluster: cluster["elasticsearch_conn_id"]))

    @task
    def insert_elasticsearch_index_stats_raw(
        stats_and_cluster: Tuple[Dict[str, Dict[str, Any]], Dict[str, str]], data_interval_end=None
    ) -> int:
        index_stats, cluster = stats_and_cluster
        rows = build_clickhouse_rows(index_stats, data_interval_end, cluster["dc"], cluster["namespace"])
        ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID).bulk_insert_rows(
            CLICKHOUSE_TABLE, rows, column_names=CLICKHOUSE_COLUMNS
        )
        log.info("Wrote ts=%s dc=%s namespace=%s", data_interval_end, cluster["dc"], cluster["namespace"])
        return len(rows)

    # Mapped instances line up by index, so zip pairs each cluster's stats with that cluster.
    insert_elasticsearch_index_stats_raw.expand(
        stats_and_cluster=collect_elasticsearch_index_stats.output.zip(clusters)
    )


cost_obs_collect_elasticsearch_ta_dag()

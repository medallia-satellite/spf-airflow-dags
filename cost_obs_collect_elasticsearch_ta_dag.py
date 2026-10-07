"""Cost Observability: collect Elasticsearch index stats.

Runs hourly and takes one snapshot of per-index resource usage from every configured
Elasticsearch cluster, stored in ClickHouse. Tasks:

- ``load_elasticsearch_clusters``: reads the clusters to collect from (see below) and fails
  early if the configuration is invalid.
- ``collect_elasticsearch_index_stats`` (one mapped instance per cluster): ``GET /_all/_stats``
  on the cluster, reduced to ``total_docs_count``, ``cpu_ms_cumulative``, ``total_mem_bytes`` and
  ``total_store_bytes`` per seaas surveys or topic-builder index, plus the ``instance_id`` and
  ``in_app_id`` parsed from the index name. Records the snapshot's ``ts`` as soon as the response comes back.
- ``insert_elasticsearch_index_stats_raw`` (one mapped instance per cluster): writes one row per
  index to ClickHouse ``spf.elasticsearch_index_stats_raw``, with the snapshot's ``ts`` and the
  cluster's ``dc`` / ``namespace``.

``ts`` is when the cluster's ``_stats`` response came back (UTC), not the run's data interval:
the stats are a snapshot of that moment and can't be backfilled, so late runs, reruns and manual
runs each record a correctly timed snapshot (a rerun or manual run adds an extra snapshot rather
than overwriting one). All indices of one snapshot share its ``ts``; each cluster gets its own.

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
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook
from airflow.providers.http.operators.http import HttpOperator

log = logging.getLogger(__name__)

# Per-deployment JSON Variable with this DC's clusters; format in the module docstring.
ELASTICSEARCH_CLUSTERS_VARIABLE = "cost_obs_elasticsearch_clusters"
ELASTICSEARCH_CLUSTER_FIELDS = ("elasticsearch_conn_id", "dc", "namespace")
CLICKHOUSE_CONN_ID = "sharedservices-clickhouse-spf-test"
CLICKHOUSE_TABLE = "spf.elasticsearch_index_stats_raw"
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

# Tenant index names, tried in order (<instance> is the instance's hostname, always *.medallia.*):
#   seaas-<in_app_id>_surveys-<instance>-<in_app_id>-<instance_id>-<suffix>
#   seaas-<in_app_id>_topic-builder-<instance>-<in_app_id>-<YYYY-MM-01>-<instance_id>-<suffix>
#     e.g. seaas-pkgdentest_topic-builder-pkgdentest.medallia.com-pkgdentest-2023-10-01-101880-0
_SEAAS_INDEX_BODY = r"-(?P<instance>[\w-]+(?:\.[\w-]+)*\.medallia(?:\.\w+)+)-(?P=in_app_id)"
_SEAAS_INDEX_TAIL = r"-(?P<instance_id>[0-9]+)-(?P<suffix>[0-9]+)$"
SEAAS_INDEX_REGEXES = [
    re.compile(r"^seaas-(?P<in_app_id>\w+)_surveys" + _SEAAS_INDEX_BODY + _SEAAS_INDEX_TAIL),
    re.compile(
        r"^seaas-(?P<in_app_id>\w+)_topic-builder" + _SEAAS_INDEX_BODY
        + r"-(?P<month>[0-9]{4}-[0-9]{2}-[0-9]{2})" + _SEAAS_INDEX_TAIL
    ),
]

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


def match_seaas_index(index: str) -> Optional[re.Match]:
    """Return the first ``SEAAS_INDEX_REGEXES`` match for ``index``, or None."""
    return next((m for m in (regex.fullmatch(index) for regex in SEAAS_INDEX_REGEXES) if m), None)


def parse_index_stats(stats_response: dict) -> Dict[str, Dict[str, Any]]:
    """Reduce a ``GET /_stats`` response to per-index stats for seaas surveys and topic-builder indices.

    Each kept index gets ``total_docs_count``, ``cpu_ms_cumulative``, ``total_mem_bytes`` and
    ``total_store_bytes`` (named like the ClickHouse columns), plus the ``instance_id`` and
    ``in_app_id`` parsed from its name with ``SEAAS_INDEX_REGEXES``. Non-``seaas-`` indices
    (system indices like ``.kibana``) are skipped silently; ``seaas-`` indices that match neither
    pattern are skipped with a warning.

    Uses the ``total`` section, so primaries and replicas are both counted.
    ``cpu_ms_cumulative`` is the time spent on search and indexing since the shards started.
    """

    def sum_fields(totals: dict, fields) -> int:
        return sum(totals.get(section, {}).get(field, 0) for section, field in fields)

    index_stats = {}
    for index, stats in stats_response.get("indices", {}).items():
        if not index.startswith("seaas-"):
            continue
        match = match_seaas_index(index)
        if not match:
            log.warning("Skipping %s: name doesn't match any seaas index pattern", index)
            continue

        instance_id, in_app_id = int(match["instance_id"]), match["in_app_id"]

        totals = stats.get("total", {})
        index_stats[index] = {
            "total_docs_count": totals.get("docs", {}).get("count", 0),
            "cpu_ms_cumulative": sum_fields(totals, CPU_FIELDS),
            "total_mem_bytes": sum_fields(totals, MEMORY_FIELDS),
            "total_store_bytes": sum_fields(totals, STORAGE_FIELDS),
            "instance_id": instance_id,
            "in_app_id": in_app_id,
        }

    log.info(
        "Collected stats for %d indices: cpu_ms_cumulative=%d total_mem_bytes=%d total_store_bytes=%d",
        len(index_stats),
        sum(s["cpu_ms_cumulative"] for s in index_stats.values()),
        sum(s["total_mem_bytes"] for s in index_stats.values()),
        sum(s["total_store_bytes"] for s in index_stats.values()),
    )

    return index_stats


def snapshot_index_stats(stats_response: dict) -> Dict[str, Any]:
    """Return ``{"ts": now (UTC), "index_stats": parse_index_stats(stats_response)}`` for one cluster.

    Called as the collect task's ``response_filter``, so ``ts`` is taken right after the cluster
    answered and becomes the ``ts`` of every row written for this snapshot.
    """
    return {"ts": datetime.now(timezone.utc), "index_stats": parse_index_stats(stats_response)}


def validate_clusters(clusters: Any) -> List[Dict[str, str]]:
    """Check the ``ELASTICSEARCH_CLUSTERS_VARIABLE`` value and return its clusters.

    Raises ``ValueError`` unless it's a non-empty list whose entries all have
    ``ELASTICSEARCH_CLUSTER_FIELDS``. Extra keys are dropped. Logs the clusters it returns.
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
    """Turn ``parse_index_stats`` output into ``elasticsearch_index_stats_raw`` rows, in ``CLICKHOUSE_COLUMNS`` order.

    ``ts`` is the snapshot time from ``snapshot_index_stats``; ``dc`` and ``namespace`` come from the cluster.
    """
    rows = []
    for index, stats in index_stats.items():
        row = {"ts": ts, "dc": dc, "namespace": namespace, "index_name": index, **stats}
        rows.append(tuple(row[column] for column in CLICKHOUSE_COLUMNS))
    return rows


@dag(
    dag_display_name="Cost Observability: Collect Elasticsearch Index Stats",
    tags=["spf", "elasticsearch", "clickhouse", "cost-observability"],
    description=(
        "Collect per-index docs/CPU/memory/storage from every configured Elasticsearch cluster "
        "into ClickHouse spf.elasticsearch_index_stats_raw."
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
        response_filter=lambda response: snapshot_index_stats(response.json()),
    ).expand(http_conn_id=clusters.map(lambda cluster: cluster["elasticsearch_conn_id"]))

    @task
    def insert_elasticsearch_index_stats_raw(
        snapshot_and_cluster: Tuple[Dict[str, Any], Dict[str, str]],
    ) -> int:
        snapshot, cluster = snapshot_and_cluster
        ts = snapshot["ts"]
        rows = build_clickhouse_rows(snapshot["index_stats"], ts, cluster["dc"], cluster["namespace"])
        ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID).bulk_insert_rows(
            CLICKHOUSE_TABLE, rows, column_names=CLICKHOUSE_COLUMNS
        )
        log.info("Wrote ts=%s dc=%s namespace=%s", ts, cluster["dc"], cluster["namespace"])
        return len(rows)

    # Mapped instances line up by index, so zip pairs each cluster's snapshot with that cluster.
    insert_elasticsearch_index_stats_raw.expand(
        snapshot_and_cluster=collect_elasticsearch_index_stats.output.zip(clusters)
    )


cost_obs_collect_elasticsearch_ta_dag()

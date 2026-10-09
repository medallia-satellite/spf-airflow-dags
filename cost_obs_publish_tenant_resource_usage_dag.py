"""Cost Observability: publish daily per-tenant resource usage.

Splits one day of each Elasticsearch cluster's measured CPU, memory and storage between its tenants
and writes the result to ClickHouse ``tenant_resource_usage_daily``. Each cluster (``namespaces``
param) is a separate deployment with its own mapped task instances, so one cluster failing doesn't
block the others. Tasks:

- ``fetch_cluster_totals``: the cluster's CPU and memory for the day from ``cost.workload_usage_daily``
  (sc4 costopt), added up over every container in the cluster's namespace, both usage and requests.
  Fails if the day has no rows, so a missing collection is never published as zero.
- ``fetch_tenant_usage``: the day's hourly ``elasticsearch_index_stats`` grouped by ``tenant_id``.
  The ``tenant_id`` comes from the materialized view that fills ``elasticsearch_index_stats``;
  indices without a tenant have ``tenant_id = 0``.
- ``publish_tenant_resource_usage``: turns both into one row per tenant and resource type and
  inserts them with the next ``revision`` for the day, so reruns never overwrite earlier results.

How each resource type is split (``weight`` is the tenant's share and adds up to 1 per
dc/service/deployment/resource_type/date, ``tenant_id = 0`` included):

| resource_type  | cluster_total                                | tenant value                                   | unit       | rung     |
|----------------|----------------------------------------------|------------------------------------------------|------------|----------|
| cpu            | CPU used by the cluster's containers         | cluster_total * tenant's share of cpu_ms       | core_hours | activity |
| cpu_request    | CPU requested by the cluster's containers    | cluster_total * tenant's share of cpu_ms       | core_hours | activity |
| memory         | memory used by the cluster's containers      | tenant's measured memory + its share of the rest by cpu_ms | gib_avg | activity |
| memory_request | memory requested by the cluster's containers | tenant's measured memory + its share of the rest by cpu_ms | gib_avg | activity |
| storage        | sum of the tenants' storage                  | tenant's measured storage                      | gib_avg    | measured |

``value = cluster_total * weight`` holds on every row.

- cpu_ms: search and indexing time spent on the tenant's indices during the day.
- memory: the memory Elasticsearch reports per index (``total_mem_bytes``: segments, caches,
  fielddata), averaged over the day, goes to its tenant; the rest of the cluster's memory (heap,
  page cache, overhead) is split by the tenant's share of cpu_ms. Elasticsearch 8 reports almost
  nothing per index, so the rung is ``activity``.
- storage: store size of the tenant's indices, measured every hour and averaged over the day
  (like storage billing), so an index deleted or created mid-day counts for the hours it existed.

The rung describes how a resource was split, so all rows of a resource on a day share it,
``tenant_id = 0`` included. If no tenant has any cpu_ms for the day, cpu and the weighted part of
memory fall back to rung ``roster``: an equal split between the known tenants (``tenant_id = 0``
gets nothing). If there are no known tenants either, everything goes to ``tenant_id = 0`` with
rung ``unattributed``.

Params ``dc`` and ``namespaces`` describe where the Elasticsearch clusters run; each namespace is
written as its ``deployment``. Every container in the namespace counts towards the cluster totals,
helpers like ``registrator`` or ``elasticsearch-shards-reporter`` included, since they run because
of the cluster. ``date`` defaults to the run's ``ds``.

Connections: ``clickhouse_costopt_sc4`` (ClickHouse, read-only) and
``sharedservices-clickhouse-spf-test`` (ClickHouse).
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

from airflow.decorators import dag, task
from airflow.models import Param
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook

log = logging.getLogger(__name__)

CLICKHOUSE_COSTOPT_CONN_ID = "clickhouse_costopt_sc4"
CLICKHOUSE_SPF_CONN_ID = "sharedservices-clickhouse-spf-test"
CLICKHOUSE_TABLE = "tenant_resource_usage_daily"
CLICKHOUSE_COLUMNS = [
    "date",
    "dc",
    "service",
    "deployment",
    "tenant_id",
    "resource_type",
    "value",
    "unit",
    "weight",
    "cluster_total",
    "rung",
    "state",
    "revision",
    "calculated_at",
]
SERVICE = "elasticsearch"
# Same convention as the elasticsearch_index_stats materialized view: 0 marks indices without a tenant.
UNATTRIBUTED_TENANT_ID = 0
GIB = 1024**3
# workload_usage_daily samples each pod once a minute (5 samples per v_container_usage_5m window):
# sample_count is pods x minutes, and the *_avg columns are per-pod averages over those samples.
# One pod running all day = 1440 samples.
SAMPLES_PER_HOUR = 60
SAMPLES_PER_DAY = 24 * SAMPLES_PER_HOUR

# avg * sample_count is core-minutes (or byte-minutes): / SAMPLES_PER_HOUR gives core-hours,
# / SAMPLES_PER_DAY gives the memory held on average over the day.
CLUSTER_TOTALS_SQL = f"""
SELECT
    count() AS workload_rows,
    sum(cpu_usage_cores_avg * sample_count) / {SAMPLES_PER_HOUR} AS cpu_core_hours,
    sum(cpu_request_cores_avg * sample_count) / {SAMPLES_PER_HOUR} AS cpu_request_core_hours,
    sum(mem_working_set_bytes_avg * sample_count) / {SAMPLES_PER_DAY} / {GIB} AS memory_gib,
    sum(mem_request_bytes_avg * sample_count) / {SAMPLES_PER_DAY} / {GIB} AS memory_request_gib
FROM cost.workload_usage_daily
WHERE dc = %(dc)s AND k8s_namespace = %(namespace)s AND window_start = %(date)s
"""

# The collector stamps each snapshot with the moment it was taken, a few seconds after the hour it
# closes (the 24:00 one lands at 00:00:02 of the next day), and reruns or manual runs add extra
# snapshots. So the day's snapshots are those in (00:30, 24:30] UTC. cpu_ms_delta covers the time
# since the index's previous snapshot, so it adds up across any number of snapshots. Memory and
# storage are averaged over the day's snapshots, so an index missing from some snapshots counts
# as 0 for those.
TENANT_USAGE_SQL = f"""
WITH
    toDateTime(%(date)s, 'UTC') AS day_start,
    day_stats AS (
        SELECT *
        FROM spf.elasticsearch_index_stats
        WHERE dc = %(dc)s AND namespace = %(namespace)s
          AND ts > day_start + INTERVAL 30 MINUTE AND ts <= day_start + INTERVAL 1 DAY + INTERVAL 30 MINUTE
    ),
    (SELECT uniqExact(ts) FROM day_stats) AS snapshots
SELECT
    tenant_id,
    sum(cpu_ms_delta) AS cpu_ms,
    sum(total_mem_bytes) / snapshots / {GIB} AS memory_gib,
    sum(total_store_bytes) / snapshots / {GIB} AS storage_gib,
    snapshots
FROM day_stats
GROUP BY tenant_id
"""
TENANT_USAGE_COLUMNS = ["tenant_id", "cpu_ms", "memory_gib", "storage_gib", "snapshots"]

NEXT_REVISION_SQL = f"""
SELECT max(revision) + 1
FROM spf.{CLICKHOUSE_TABLE}
WHERE date = %(date)s AND dc = %(dc)s AND service = '{SERVICE}' AND deployment = %(namespace)s
"""

# resource_type -> (cluster total key, tenant's measured usage key or None, tenant usage key used as weight, unit)
WEIGHTED_RESOURCES = {
    "cpu": ("cpu_core_hours", None, "cpu_ms", "core_hours"),
    "cpu_request": ("cpu_request_core_hours", None, "cpu_ms", "core_hours"),
    "memory": ("memory_gib", "memory_gib", "cpu_ms", "gib_avg"),
    "memory_request": ("memory_request_gib", "memory_gib", "cpu_ms", "gib_avg"),
}


def compute_weights(tenant_usage: List[Dict[str, Any]], key: str) -> Tuple[List[float], str]:
    """Each tenant's share of ``key``, in ``tenant_usage`` order, and the rung it was computed with.

    Shares add up to 1. If no tenant has any ``key``, falls back to rung ``roster``: an equal split
    between the known tenants. If none is known, everything goes to ``tenant_id = 0`` as ``unattributed``.
    """
    total = sum(tenant[key] for tenant in tenant_usage)
    if total:
        return [tenant[key] / total for tenant in tenant_usage], "activity"
    members = [tenant["tenant_id"] != UNATTRIBUTED_TENANT_ID for tenant in tenant_usage]
    if not any(members):
        log.warning("No %s recorded and no known tenants; attributing everything to tenant_id 0", key)
        return [1.0 if tenant["tenant_id"] == UNATTRIBUTED_TENANT_ID else 0.0 for tenant in tenant_usage], "unattributed"
    log.warning("No %s recorded for any tenant; splitting equally between %d known tenants", key, sum(members))
    return [member / sum(members) for member in members], "roster"


def build_clickhouse_rows(
    cluster_totals: Dict[str, float],
    tenant_usage: List[Dict[str, Any]],
    day: date,
    dc: str,
    namespace: str,
    revision: int,
    calculated_at: datetime,
) -> List[tuple]:
    """Turn the cluster totals and per-tenant usage into ``tenant_resource_usage_daily`` rows, in ``CLICKHOUSE_COLUMNS`` order."""
    rows = []

    def add_row(tenant: Dict[str, Any], resource_type: str, value: float, unit: str, weight: float, total: float, rung: str):
        rows.append(
            (
                day,
                dc,
                SERVICE,
                namespace,
                tenant["tenant_id"],
                resource_type,
                value,
                unit,
                weight,
                total,
                rung,
                "provisional",
                revision,
                calculated_at,
            )
        )

    for resource_type, (total_key, measured_key, weight_key, unit) in WEIGHTED_RESOURCES.items():
        cluster_total = cluster_totals[total_key]
        measured = [tenant[measured_key] if measured_key else 0.0 for tenant in tenant_usage]
        rest = cluster_total - sum(measured)
        if rest < 0:
            log.warning("Measured %s (%f) exceeds the cluster total (%f); nothing left to weight", resource_type, sum(measured), cluster_total)
            rest = 0.0
        weights, rung = compute_weights(tenant_usage, weight_key)
        for tenant, tenant_measured, weight in zip(tenant_usage, measured, weights):
            value = tenant_measured + rest * weight
            add_row(tenant, resource_type, value, unit, value / cluster_total if cluster_total else 0.0, cluster_total, rung)

    storage_total = sum(tenant["storage_gib"] for tenant in tenant_usage)
    for tenant in tenant_usage:
        weight = tenant["storage_gib"] / storage_total if storage_total else 0.0
        add_row(tenant, "storage", tenant["storage_gib"], "gib_avg", weight, storage_total, "measured")

    return rows


@dag(
    dag_display_name="Cost Observability: Publish Tenant Resource Usage",
    tags=["spf", "elasticsearch", "clickhouse", "cost-observability"],
    description="Split an Elasticsearch cluster's daily CPU/memory/storage between tenants into ClickHouse tenant_resource_usage_daily.",
    doc_md=__doc__,
    max_active_runs=1,
    start_date=datetime(2026, 1, 1),
    # workload_usage_daily has the previous day by then; a missing day fails and is retried.
    schedule="0 6 * * *",
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(hours=1)},
    render_template_as_native_obj=True,
    params={
        # Where the Elasticsearch clusters run; each namespace is published as its own deployment.
        "dc": Param("den", type="string"),
        "namespaces": Param(
            ["sharedservices-elasticsearch", "sharedservices-elasticsearch-wordtags"], type="array"
        ),
        # Day to publish (YYYY-MM-DD); empty means the run's ds.
        "date": Param("", type="string"),
    },
)
def cost_obs_publish_tenant_resource_usage_dag():
    @task
    def query_params(ds=None, params=None) -> List[Dict[str, Any]]:
        """One query per namespace."""
        return [
            {"dc": params["dc"], "namespace": namespace, "date": params["date"] or ds}
            for namespace in params["namespaces"]
        ]

    @task
    def fetch_cluster_totals(query: Dict[str, Any]) -> Dict[str, float]:
        hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_COSTOPT_CONN_ID)
        workload_rows, *totals = hook.get_first(CLUSTER_TOTALS_SQL, parameters=query)
        if not workload_rows:
            raise ValueError(f"No workload_usage_daily rows for {query}; not publishing")
        cluster_totals = dict(
            zip(["cpu_core_hours", "cpu_request_core_hours", "memory_gib", "memory_request_gib"], totals)
        )
        log.info("Cluster totals for %s from %d workload rows: %s", query, workload_rows, cluster_totals)
        return cluster_totals

    @task
    def fetch_tenant_usage(query: Dict[str, Any]) -> List[Dict[str, Any]]:
        hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_SPF_CONN_ID)
        tenant_usage = [dict(zip(TENANT_USAGE_COLUMNS, row)) for row in hook.get_records(TENANT_USAGE_SQL, parameters=query)]
        if not tenant_usage:
            raise ValueError(f"No elasticsearch_index_stats for {query}; not publishing")
        snapshots = tenant_usage[0]["snapshots"]
        if snapshots < 24:
            log.warning("Only %d snapshots in elasticsearch_index_stats for %s, expected 24", snapshots, query)
        log.info(
            "%d tenants over %d snapshots, unattributed indices: %s",
            len(tenant_usage),
            snapshots,
            any(tenant["tenant_id"] == UNATTRIBUTED_TENANT_ID for tenant in tenant_usage),
        )
        return tenant_usage

    @task
    def publish_tenant_resource_usage(
        query_and_usage: Tuple[Dict[str, Any], Dict[str, float], List[Dict[str, Any]]],
    ) -> int:
        query, cluster_totals, tenant_usage = query_and_usage
        hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_SPF_CONN_ID)
        revision = hook.get_first(NEXT_REVISION_SQL, parameters=query)[0]
        rows = build_clickhouse_rows(
            cluster_totals,
            tenant_usage,
            date.fromisoformat(query["date"]),
            query["dc"],
            query["namespace"],
            revision,
            datetime.now(timezone.utc),
        )
        hook.bulk_insert_rows(CLICKHOUSE_TABLE, rows, column_names=CLICKHOUSE_COLUMNS)
        log.info("Wrote %d rows for %s as revision %d", len(rows), query, revision)
        return len(rows)

    queries = query_params()
    # Mapped instances line up by index, so zip pairs each namespace's query with its totals and usage.
    publish_tenant_resource_usage.expand(
        query_and_usage=queries.zip(fetch_cluster_totals.expand(query=queries), fetch_tenant_usage.expand(query=queries))
    )


cost_obs_publish_tenant_resource_usage_dag()

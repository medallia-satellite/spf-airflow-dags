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

The queries are in ``cost_obs/sql/`` and the rung functions in ``cost_obs/rungs.py``.

Connections: ``clickhouse_costopt_sc4`` (ClickHouse, read-only) and
``sharedservices-clickhouse-spf-test`` (ClickHouse).
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

from airflow.decorators import dag, task
from airflow.models import Param
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook

from cost_obs.queries import load_query
from cost_obs.rungs import UNATTRIBUTED_TENANT_ID, Split, rung_activity, rung_measured

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

CLUSTER_TOTALS_SQL = load_query("workload_cluster_totals")
CLUSTER_TOTALS_COLUMNS = ["cpu_core_hours", "cpu_request_core_hours", "memory_gib", "memory_request_gib"]
TENANT_USAGE_SQL = load_query("elasticsearch_tenant_usage")
TENANT_USAGE_COLUMNS = ["tenant_id", "cpu_ms", "memory_gib", "storage_gib", "snapshots"]
NEXT_REVISION_SQL = load_query("next_revision")

# resource_type -> (how it's split between tenants, unit); the rung_* keys are cluster totals and tenant usage columns.
RESOURCES: Dict[str, Tuple[Split, str]] = {
    "cpu": (rung_activity("cpu_core_hours", "cpu_ms"), "core_hours"),
    "cpu_request": (rung_activity("cpu_request_core_hours", "cpu_ms"), "core_hours"),
    "memory": (rung_activity("memory_gib", "cpu_ms", measured_key="memory_gib"), "gib_avg"),
    "memory_request": (rung_activity("memory_request_gib", "cpu_ms", measured_key="memory_gib"), "gib_avg"),
    "storage": (rung_measured("storage_gib"), "gib_avg"),
}


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

    for resource_type, (split, unit) in RESOURCES.items():
        cluster_total, values, rung = split(cluster_totals, tenant_usage)
        for tenant, value in zip(tenant_usage, values):
            add_row(tenant, resource_type, value, unit, value / cluster_total if cluster_total else 0.0, cluster_total, rung)

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
        cluster_totals = dict(zip(CLUSTER_TOTALS_COLUMNS, totals))
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
        revision = hook.get_first(NEXT_REVISION_SQL, parameters={**query, "service": SERVICE})[0]
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

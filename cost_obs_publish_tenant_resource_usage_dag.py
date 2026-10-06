"""Cost Observability: publish daily per-tenant resource usage.

Splits one day of an Elasticsearch cluster's measured CPU, memory and storage between its tenants
and writes the result to ClickHouse ``tenant_resource_usage_daily``. Tasks:

- ``fetch_cluster_totals``: the cluster's CPU and memory for the day from ``cost.workload_usage_daily``
  (sc4 costopt), added up over every workload in the namespace, both usage and requests.
  Fails if the day has no rows, so a missing collection is never published as zero.
- ``fetch_tenant_usage``: the day's hourly ``elasticsearch_index_stats`` grouped by
  ``(tenant_id, instance_id, in_app_id)``. The tenant is looked up in ``tenant_registry_mapping``
  at publish time instead of using the ``tenant_id`` stored with the stats, so a corrected mapping
  is picked up by the next run. Indices without a tenant get ``tenant_id = 0``.
- ``publish_tenant_resource_usage``: turns both into one row per tenant and resource type and
  inserts them with the next ``revision`` for the day, so reruns never overwrite earlier results.

How each resource type is split (``weight`` is the tenant's share and adds up to 1 per
dc/service/deployment/resource_type/date, ``tenant_id = 0`` included):

==================  =========================================  ===========  ==============
resource_type       weight from                                value        rung
==================  =========================================  ===========  ==============
cpu                 cpu_ms spent on the tenant's indices       core-hours   activity
cpu_request         same as cpu                                core-hours   activity
memory              same as storage                            GiB average  footprint
memory_request      same as storage                            GiB average  footprint
storage             hourly average of the indices' store size  GiB          measured
==================  =========================================  ===========  ==============

For cpu and memory, ``value = cluster_total * weight``. Storage is measured per index, so
``value`` is the tenant's own GiB and ``cluster_total`` the sum over all indices.
Memory is weighted by storage because Elasticsearch 8 reports 0 for the per-index memory stats
(``total_mem_bytes``), and heap and page cache grow with the data an index holds.
Rows with ``tenant_id = 0`` always get rung ``unattributed``.

Params ``dc`` and ``namespace`` describe where the Elasticsearch cluster runs; ``namespace`` is
written as ``deployment``. ``date`` defaults to the run's ``ds``.

Connections: ``clickhouse_costopt_sc4`` (ClickHouse, read-only) and
``sharedservices-clickhouse-spf-test`` (ClickHouse).
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.models import Param
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook

log = logging.getLogger(__name__)

COSTOPT_CONN_ID = "clickhouse_costopt_sc4"
CLICKHOUSE_CONN_ID = "sharedservices-clickhouse-spf-test"
CLICKHOUSE_TABLE = "tenant_resource_usage_daily"
CLICKHOUSE_COLUMNS = [
    "date",
    "dc",
    "service",
    "deployment",
    "tenant_id",
    "instance_id",
    "in_app_id",
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
# Same convention as costobs_poc_dag: 0 marks indices whose (instance_id, in_app_id) has no tenant.
UNATTRIBUTED_TENANT_ID = 0
GIB = 1024**3
# workload_usage_daily samples each pod once a minute: sample_count is pods x minutes, and the
# *_avg columns are per-pod averages over those samples. One pod running all day = 1440 samples.
SAMPLES_PER_DAY = 1440

# Every workload in the namespace counts towards the cluster total, sidecars included.
# avg * sample_count / SAMPLES_PER_DAY is the per-pod average times the pod-days the workload ran.
CLUSTER_TOTALS_SQL = f"""
SELECT
    count() AS workload_rows,
    sum(cpu_usage_cores_avg * sample_count) / {SAMPLES_PER_DAY} * 24 AS cpu_core_hours,
    sum(cpu_request_cores_avg * sample_count) / {SAMPLES_PER_DAY} * 24 AS cpu_request_core_hours,
    sum(mem_working_set_bytes_avg * sample_count) / {SAMPLES_PER_DAY} / {GIB} AS memory_gib,
    sum(mem_request_bytes_avg * sample_count) / {SAMPLES_PER_DAY} / {GIB} AS memory_request_gib
FROM cost.workload_usage_daily
WHERE dc = %(dc)s AND k8s_namespace = %(namespace)s AND window_start = %(date)s
"""

# Hourly stats are stamped with the end of their hour, so the day is (00:00, 24:00] UTC.
# Storage is averaged over every hour that has stats, so an index missing
# from some hours counts as 0 for those hours.
TENANT_USAGE_SQL = """
WITH
    toDateTime(%(date)s, 'UTC') AS day_start,
    day_stats AS (
        SELECT *
        FROM spf.elasticsearch_index_stats
        WHERE dc = %(dc)s AND namespace = %(namespace)s
          AND ts > day_start AND ts <= day_start + INTERVAL 1 DAY
    ),
    (SELECT uniqExact(ts) FROM day_stats) AS hours
SELECT
    m.tenant_id AS tenant_id,
    s.instance_id AS instance_id,
    s.in_app_id AS in_app_id,
    sum(s.cpu_ms_delta) AS cpu_ms,
    sum(s.total_store_bytes) / hours AS storage_bytes,
    hours
FROM day_stats AS s
ANY LEFT JOIN spf.tenant_registry_mapping AS m
    ON s.instance_id = m.instance_id AND s.in_app_id = m.in_app_id
GROUP BY tenant_id, instance_id, in_app_id
"""
TENANT_USAGE_COLUMNS = ["tenant_id", "instance_id", "in_app_id", "cpu_ms", "storage_bytes", "hours"]

NEXT_REVISION_SQL = f"""
SELECT max(revision) + 1
FROM spf.{CLICKHOUSE_TABLE}
WHERE date = %(date)s AND dc = %(dc)s AND service = '{SERVICE}' AND deployment = %(namespace)s
"""

# resource_type -> (cluster total key, tenant usage key used as weight, unit, rung)
WEIGHTED_RESOURCES = {
    "cpu": ("cpu_core_hours", "cpu_ms", "core_hours", "activity"),
    "cpu_request": ("cpu_request_core_hours", "cpu_ms", "core_hours", "activity"),
    "memory": ("memory_gib", "storage_bytes", "gib_avg", "footprint"),
    "memory_request": ("memory_request_gib", "storage_bytes", "gib_avg", "footprint"),
}


def compute_weights(tenant_usage: List[Dict[str, Any]], key: str) -> List[float]:
    """Each tenant's share of ``key``, in ``tenant_usage`` order. Adds up to 1 unless the total is 0."""
    total = sum(tenant[key] for tenant in tenant_usage)
    return [tenant[key] / total if total else 0.0 for tenant in tenant_usage]


def build_clickhouse_rows(
    cluster_totals: Dict[str, float],
    tenant_usage: List[Dict[str, Any]],
    day: date,
    dc: str,
    namespace: str,
    revision: int,
    calculated_at: datetime,
) -> List[tuple]:
    """Turn the cluster totals and per-tenant usage into ``tenant_resource_usage_daily`` rows, in ``CLICKHOUSE_COLUMNS`` order.

    If nothing can be weighted for a resource type (e.g. no cpu_ms recorded all day), the whole
    cluster total goes to a single ``tenant_id = 0`` row so the published total still matches.
    """
    rows = []

    def add_row(tenant: Dict[str, Any], resource_type: str, value: float, unit: str, weight: float, total: float, rung: str):
        if tenant["tenant_id"] == UNATTRIBUTED_TENANT_ID:
            rung = "unattributed"
        rows.append(
            (
                day,
                dc,
                SERVICE,
                namespace,
                tenant["tenant_id"],
                tenant["instance_id"],
                tenant["in_app_id"],
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

    unattributed = {"tenant_id": UNATTRIBUTED_TENANT_ID, "instance_id": 0, "in_app_id": ""}

    for resource_type, (total_key, weight_key, unit, rung) in WEIGHTED_RESOURCES.items():
        cluster_total = cluster_totals[total_key]
        weights = compute_weights(tenant_usage, weight_key)
        if not any(weights):
            log.warning("No %s recorded for any tenant; attributing all %s to tenant_id 0", weight_key, resource_type)
            add_row(unattributed, resource_type, cluster_total, unit, 1.0, cluster_total, rung)
            continue
        for tenant, weight in zip(tenant_usage, weights):
            add_row(tenant, resource_type, cluster_total * weight, unit, weight, cluster_total, rung)

    storage_total = sum(tenant["storage_bytes"] for tenant in tenant_usage) / GIB
    for tenant, weight in zip(tenant_usage, compute_weights(tenant_usage, "storage_bytes")):
        add_row(tenant, "storage", tenant["storage_bytes"] / GIB, "gib", weight, storage_total, "measured")

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
        # Where the Elasticsearch cluster runs.
        "dc": Param("den", type="string"),
        "namespace": Param("sharedservices-elasticsearch", type="string"),
        # Day to publish (YYYY-MM-DD); empty means the run's ds.
        "date": Param("", type="string"),
    },
)
def cost_obs_publish_tenant_resource_usage_dag():
    @task
    def query_params(ds=None, params=None) -> Dict[str, str]:
        return {"dc": params["dc"], "namespace": params["namespace"], "date": params["date"] or ds}

    @task
    def fetch_cluster_totals(query: Dict[str, str]) -> Dict[str, float]:
        hook = ClickHouseHook(clickhouse_conn_id=COSTOPT_CONN_ID)
        workload_rows, *totals = hook.get_first(CLUSTER_TOTALS_SQL, parameters=query)
        if not workload_rows:
            raise ValueError(f"No workload_usage_daily rows for {query}; not publishing")
        cluster_totals = dict(
            zip(["cpu_core_hours", "cpu_request_core_hours", "memory_gib", "memory_request_gib"], totals)
        )
        log.info("Cluster totals for %s from %d workload rows: %s", query, workload_rows, cluster_totals)
        return cluster_totals

    @task
    def fetch_tenant_usage(query: Dict[str, str]) -> List[Dict[str, Any]]:
        hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
        tenant_usage = [dict(zip(TENANT_USAGE_COLUMNS, row)) for row in hook.get_records(TENANT_USAGE_SQL, parameters=query)]
        if not tenant_usage:
            raise ValueError(f"No elasticsearch_index_stats for {query}; not publishing")
        hours = tenant_usage[0]["hours"]
        if hours < 24:
            log.warning("Only %d of 24 hours have elasticsearch_index_stats for %s", hours, query)
        log.info(
            "%d (tenant, instance, in_app_id) groups over %d hours, %d without a tenant",
            len(tenant_usage),
            hours,
            sum(1 for tenant in tenant_usage if tenant["tenant_id"] == UNATTRIBUTED_TENANT_ID),
        )
        return tenant_usage

    @task
    def publish_tenant_resource_usage(
        query: Dict[str, str], cluster_totals: Dict[str, float], tenant_usage: List[Dict[str, Any]]
    ) -> int:
        hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
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

    query = query_params()
    publish_tenant_resource_usage(query, fetch_cluster_totals(query), fetch_tenant_usage(query))


cost_obs_publish_tenant_resource_usage_dag()

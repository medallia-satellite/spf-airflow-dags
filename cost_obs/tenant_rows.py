"""Cost Observability: ``tenant_resource_usage_daily`` rows from a day's cluster totals and tenant usage.

Kept out of the publish DAG file so it can be imported (and tested) without Airflow.
"""

from datetime import date, datetime
from typing import Any, Dict, List, Tuple

from cost_obs.rungs import Split, rung_activity, rung_measured

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

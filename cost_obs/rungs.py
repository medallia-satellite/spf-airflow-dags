"""Cost Observability: how a resource is split between tenants, one function per rung.

Each ``rung_*`` function takes the keys it needs and returns a ``Split``: a function that gets the
deployment's ``cluster_totals`` and ``tenant_usage`` for one day and returns the cluster total,
each tenant's value (in ``tenant_usage`` order) and the rung actually used. Rungs, from most to
least precise (Per Tenant Design doc): measured, activity, footprint, roster, unattributed.

``cluster_totals`` is a dict of totals for the deployment; ``tenant_usage`` is one dict per tenant
with its ``tenant_id`` and usage columns. The keys passed to ``rung_*`` name entries of those dicts,
so the functions work the same for any service.
"""

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

# 0 marks usage without a tenant (same convention as the ClickHouse tables).
UNATTRIBUTED_TENANT_ID = 0

# split(cluster_totals, tenant_usage) -> (cluster_total, each tenant's value in tenant_usage order, rung)
Split = Callable[[Dict[str, float], List[Dict[str, Any]]], Tuple[float, List[float], str]]


def compute_weights(tenant_usage: List[Dict[str, Any]], key: str, rung: str) -> Tuple[List[float], str]:
    """Each tenant's share of ``key``, in ``tenant_usage`` order, and the rung it was computed with.

    Shares add up to 1 and get ``rung``. If no tenant has any ``key``, falls back to rung ``roster``:
    an equal split between the known tenants. If none is known, everything goes to ``tenant_id = 0``
    as ``unattributed``.
    """
    total = sum(tenant[key] for tenant in tenant_usage)
    if total:
        return [tenant[key] / total for tenant in tenant_usage], rung
    members = [tenant["tenant_id"] != UNATTRIBUTED_TENANT_ID for tenant in tenant_usage]
    if not any(members):
        log.warning("No %s recorded and no known tenants; attributing everything to tenant_id 0", key)
        return [1.0 if tenant["tenant_id"] == UNATTRIBUTED_TENANT_ID else 0.0 for tenant in tenant_usage], "unattributed"
    log.warning("No %s recorded for any tenant; splitting equally between %d known tenants", key, sum(members))
    return [member / sum(members) for member in members], "roster"


def rung_measured(usage_key: str) -> Split:
    """Rung ``measured``: each tenant gets its own ``usage_key``, and the cluster total is their sum."""

    def split(cluster_totals: Dict[str, float], tenant_usage: List[Dict[str, Any]]) -> Tuple[float, List[float], str]:
        values = [tenant[usage_key] for tenant in tenant_usage]
        return sum(values), values, "measured"

    return split


def _proportional(rung: str, total_key: str, weight_key: str, measured_key: Optional[str]) -> Split:
    """The cluster's ``total_key`` split by each tenant's share of ``weight_key``, written as ``rung``.

    With ``measured_key``, each tenant first gets its own measured usage and only the rest of the
    cluster total is split. Falls back to ``roster`` or ``unattributed`` as in ``compute_weights``.
    """

    def split(cluster_totals: Dict[str, float], tenant_usage: List[Dict[str, Any]]) -> Tuple[float, List[float], str]:
        cluster_total = cluster_totals[total_key]
        measured = [tenant[measured_key] if measured_key else 0.0 for tenant in tenant_usage]
        rest = cluster_total - sum(measured)
        if rest < 0:
            log.warning("Measured %s (%f) exceeds the cluster's %s (%f); nothing left to weight", measured_key, sum(measured), total_key, cluster_total)
            rest = 0.0
        weights, used_rung = compute_weights(tenant_usage, weight_key, rung)
        return cluster_total, [tenant_measured + rest * weight for tenant_measured, weight in zip(measured, weights)], used_rung

    return split


def rung_activity(total_key: str, activity_key: str, measured_key: Optional[str] = None) -> Split:
    """Rung ``activity``: the cluster's ``total_key`` split by how much work each tenant caused (``activity_key``).

    See ``_proportional`` for ``measured_key`` and the fallbacks.
    """
    return _proportional("activity", total_key, activity_key, measured_key)


def rung_footprint(total_key: str, size_key: str, measured_key: Optional[str] = None) -> Split:
    """Rung ``footprint``: the cluster's ``total_key`` split by how much each tenant holds (``size_key``).

    For when there's no per-tenant activity, e.g. stored bytes or key counts. See ``_proportional``
    for ``measured_key`` and the fallbacks.
    """
    return _proportional("footprint", total_key, size_key, measured_key)

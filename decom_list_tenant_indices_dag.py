"""Decom: list the Elasticsearch indices of a decommissioned tenant.

Manually triggered, one tenant per run, identified by the ``hostname`` of its Express instance and
its ``tenant`` name (its ``in_app_id`` in Tenant Registry and index names). Lists the tenant's
seaas surveys and topic-builder indices (patterns in ``seaas_indices``) on every Elasticsearch
cluster in the ``orchestrate_dag_targets`` Variable; it doesn't delete anything. Tasks:

- ``load_clusters``: reads the clusters from ``orchestrate_dag_targets`` (see below) and fails
  early if the Variable is invalid.

- ``fetch_tenant``: looks the tenant up in Tenant Registry
  (``GET /api/v0/applications/id/com.medallia.express/instances/``) to get its ``instance_id``
  (the instance's ``tenant_id``) and ``tenant_id``. Fails if it isn't there. Tenant Registry
  doesn't know whether a tenant is decommissioned; that's up to whoever triggers the run.
- ``list_indices`` (one mapped instance per cluster): lists the cluster's indices
  (``GET /_cat/indices``), logs and returns the seaas indices whose ``in_app_id``, hostname and
  ``instance_id`` all match the tenant's. Indices named for the tenant but with another
  ``instance_id`` are logged separately and not returned.
- ``summarize``: logs every cluster's indices together, with counts.
- ``check_snapshot`` (one mapped instance per cluster, after its ``list_indices``): skipped on
  clusters without indices of the tenant. On the others, lists the snapshots in the
  ``es-snapshots`` repository (``GET /_cat/snapshots/es-snapshots``) and fails unless one with
  status ``SUCCESS`` started on the run's date (``ds``, UTC), i.e. the day it was triggered.

``orchestrate_dag_targets`` is the JSON Variable ``orchestrate_maintenance_dag`` also uses: one
entry per cluster, keyed by its name. Only ``conn_id`` is used here::

    {
        "wordtags": {"conn_id": "es-wordtags", "dry_run": false},
        "wordtags-qa": {"conn_id": "es-wordtags-qa", "dry_run": false}
    }

Connections: each cluster's ``conn_id`` (Elasticsearch, HTTP) and ``tenant-registry`` (HTTP).
"""

import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Param, Variable
from airflow.operators.python import get_current_context

from seaas_indices import match_seaas_index
from utils import http_hook_get, param_value

log = logging.getLogger(__name__)

TENANT_REGISTRY_CONN_ID = "tenant-registry"
TENANT_REGISTRY_EXPRESS_INSTANCES_ENDPOINT = "/api/v0/applications/id/com.medallia.express/instances/"
# Per-deployment JSON Variable with the clusters to look in; format in the module docstring.
CLUSTERS_VARIABLE = "orchestrate_dag_targets"
SNAPSHOT_REPOSITORY = "es-snapshots"


def validate_clusters(targets: Any) -> List[Dict[str, str]]:
    """Turn the ``CLUSTERS_VARIABLE`` value into ``[{"name", "conn_id"}]``, sorted by name.

    Raises ``ValueError`` unless it's a non-empty JSON object whose entries all have a ``conn_id``.
    """
    if not isinstance(targets, dict) or not targets:
        raise ValueError(f"Variable {CLUSTERS_VARIABLE} must be a non-empty JSON object of clusters")
    missing = sorted(
        name for name, target in targets.items() if not isinstance(target, dict) or not target.get("conn_id")
    )
    if missing:
        raise ValueError(f"Variable {CLUSTERS_VARIABLE} entries without a conn_id: {missing}")
    clusters = [{"name": name, "conn_id": targets[name]["conn_id"]} for name in sorted(targets)]
    log.info("Looking in %d Elasticsearch clusters: %s", len(clusters), clusters)
    return clusters


def find_tenant(instances_response: dict, hostname: str, tenant: str) -> Dict[str, Any]:
    """Find ``tenant`` (its ``in_app_id``) on the Express instance ``hostname`` in a Tenant Registry instances response.

    Returns ``{"hostname", "tenant", "instance_id", "tenant_id"}``; ``instance_id`` is the
    instance's top-level ``tenant_id``. Raises ``ValueError`` if ``hostname`` or ``tenant`` is
    empty or the tenant isn't in the response.
    """
    if not hostname or not tenant:
        raise ValueError("Set both hostname and tenant")

    instances = instances_response.get("items", [])
    for instance in instances:
        if instance.get("hostname") != hostname:
            continue
        for registry_tenant in instance.get("tenants", []):
            if registry_tenant.get("in_app_id") == tenant:
                found = {
                    "hostname": hostname,
                    "tenant": tenant,
                    "instance_id": instance["tenant_id"],
                    "tenant_id": registry_tenant["tenant_id"],
                }
                log.info("Found tenant in Tenant Registry: %s", found)
                return found
        raise ValueError(f"Instance {hostname} has no tenant {tenant} in Tenant Registry")

    error = f"No Express instance with hostname {hostname} in Tenant Registry"
    if instances_response.get("_total", len(instances)) != len(instances):
        error += f" (it returned only {len(instances)} of {instances_response['_total']} instances)"
    raise ValueError(error)


def select_indices(index_names: Iterable[str], registry_tenant: Dict[str, Any]) -> List[str]:
    """Return the seaas indices in ``index_names`` whose in_app_id, hostname and instance_id match ``find_tenant``'s result, sorted.

    Indices whose in_app_id and hostname match but instance_id doesn't are logged and not returned.
    """
    selected, other_instance = [], []
    for index in index_names:
        match = match_seaas_index(index)
        if (
            not match
            or match["in_app_id"] != registry_tenant["tenant"]
            or match["instance"] != registry_tenant["hostname"]
        ):
            continue
        if int(match["instance_id"]) == registry_tenant["instance_id"]:
            selected.append(index)
        else:
            other_instance.append(index)

    if other_instance:
        log.warning(
            "Skipping %d indices named for %s on %s but with an instance_id other than %d:\n%s",
            len(other_instance),
            registry_tenant["tenant"],
            registry_tenant["hostname"],
            registry_tenant["instance_id"],
            "\n".join(sorted(other_instance)),
        )
    if not selected:
        log.warning("No indices found for %s on %s", registry_tenant["tenant"], registry_tenant["hostname"])
    return sorted(selected)


def find_snapshots_started_on(snapshots: List[Dict[str, Any]], day: date) -> List[str]:
    """Return the ids of the ``SUCCESS`` snapshots in a ``GET /_cat/snapshots`` response that started on ``day`` (UTC).

    Snapshots that started that day with another status (``IN_PROGRESS``, ``PARTIAL``, ``FAILED``)
    are logged and not returned.
    """
    found = []
    for snapshot in snapshots:
        if datetime.fromtimestamp(int(snapshot["start_epoch"]), tz=timezone.utc).date() != day:
            continue
        if snapshot["status"] == "SUCCESS":
            found.append(snapshot["id"])
        else:
            log.warning("Ignoring snapshot %s from %s: status %s", snapshot["id"], day, snapshot["status"])
    return found


@dag(
    dag_display_name="Decom: List Tenant Indices",
    tags=["spf", "elasticsearch", "decom"],
    description="List the seaas surveys and topic-builder indices of a decommissioned tenant.",
    doc_md=__doc__,
    max_active_runs=1,
    schedule=None,
    catchup=False,
    params={
        "hostname": Param("", type="string", description="Hostname of the tenant's Express instance, e.g. acme.medallia.com"),
        "tenant": Param("", type="string", description="The tenant's name (its in_app_id), e.g. acme"),
    },
    render_template_as_native_obj=True,
)
def decom_list_tenant_indices_dag():
    @task
    def fetch_tenant() -> Dict[str, Any]:
        instances = http_hook_get(TENANT_REGISTRY_CONN_ID, TENANT_REGISTRY_EXPRESS_INSTANCES_ENDPOINT)
        return find_tenant(instances, param_value("hostname"), param_value("tenant"))

    @task
    def load_clusters() -> List[Dict[str, str]]:
        return validate_clusters(Variable.get(CLUSTERS_VARIABLE, deserialize_json=True))

    # One mapped instance per cluster, labelled with the cluster's name in the UI.
    @task(map_index_template="{{ cluster_name }}")
    def list_indices(cluster: Dict[str, str], registry_tenant: Dict[str, Any]) -> Dict[str, Any]:
        get_current_context()["cluster_name"] = cluster["name"]
        fetched = http_hook_get(cluster["conn_id"], "/_cat/indices", params={"h": "index", "format": "json"})
        indices = select_indices([r["index"] for r in fetched], registry_tenant)
        log.info("%d indices for %s on %s:\n%s", len(indices), registry_tenant, cluster["name"], "\n".join(indices))
        return {**cluster, "indices": indices}

    @task
    def summarize(registry_tenant: Dict[str, Any], per_cluster: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        lines = [f"{c['name']} ({c['conn_id']}): {len(c['indices'])} indices" for c in per_cluster]
        lines += [f"  {c['name']}: {index}" for c in per_cluster for index in c["indices"]]
        log.info(
            "%d indices for %s across %d clusters:\n%s",
            sum(len(c["indices"]) for c in per_cluster),
            registry_tenant,
            len(per_cluster),
            "\n".join(lines),
        )
        return {c["name"]: c["indices"] for c in per_cluster}

    @task(map_index_template="{{ cluster_name }}")
    def check_snapshot(cluster: Dict[str, Any]) -> Dict[str, Any]:
        context = get_current_context()
        context["cluster_name"] = cluster["name"]
        if not cluster["indices"]:
            raise AirflowSkipException(f"No indices of the tenant on {cluster['name']}")
        day = date.fromisoformat(context["ds"])
        snapshots = http_hook_get(
            cluster["conn_id"], f"/_cat/snapshots/{SNAPSHOT_REPOSITORY}", params={"format": "json"}
        )
        found = find_snapshots_started_on(snapshots, day)
        if not found:
            raise ValueError(f"No successful snapshot in {SNAPSHOT_REPOSITORY} on {cluster['name']} started on {day}")
        log.info("Successful snapshots in %s on %s started on %s: %s", SNAPSHOT_REPOSITORY, cluster["name"], day, found)
        return {"name": cluster["name"], "conn_id": cluster["conn_id"], "snapshots": found}

    registry_tenant = fetch_tenant()
    clusters = load_clusters()
    per_cluster = list_indices.partial(registry_tenant=registry_tenant).expand(cluster=clusters)
    summarize(registry_tenant, per_cluster)
    check_snapshot.expand(cluster=per_cluster)


decom_list_tenant_indices_dag()

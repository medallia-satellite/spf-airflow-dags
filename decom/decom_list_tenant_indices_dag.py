"""Decom: list the Elasticsearch indices of a decommissioned tenant.

Manually triggered, one tenant per run, identified by the ``hostname`` of its Express instance and
its ``tenant`` name (its ``in_app_id`` in Tenant Registry and index names). Lists the tenant's
seaas surveys and topic-builder indices (patterns in ``seaas_indices``) on every Elasticsearch
cluster in the ``elasticsearch_clusters`` Variable that lists this DAG in its ``active_dags``,
and makes sure each cluster holding any of them has a snapshot from that day. It doesn't delete
anything. Tasks:

- ``load_clusters``: reads the clusters from ``elasticsearch_clusters`` (see below) and keeps the
  ones whose ``active_dags`` include this DAG. Fails early if the Variable is invalid or no
  cluster has this DAG active.
- ``fetch_tenant``: looks the tenant up in Tenant Registry
  (``GET /api/v0/applications/id/com.medallia.express/instances/``) to get its ``instance_id``
  (the instance's ``tenant_id``) and ``tenant_id``. Fails if it isn't there. Tenant Registry
  doesn't know whether a tenant is decommissioned; that's up to whoever triggers the run.
- ``cluster`` (task group mapped over the clusters; its tasks are labelled with the cluster's
  ``conn_id`` and only depend on the same cluster's tasks):

  - ``list_indices``: lists the cluster's indices (``GET /_cat/indices``), logs and returns the
    seaas indices whose ``in_app_id``, hostname and ``instance_id`` all match the tenant's.
    Indices named for the tenant but with another ``instance_id`` are logged separately and not
    returned.
  - ``check_snapshot``: skipped if the cluster has none of the tenant's indices. Otherwise lists
    the snapshots in the ``es-snapshots`` repository (``GET /_cat/snapshots/es-snapshots``) that
    started on the run's date (``ds``, UTC), i.e. the day it was triggered, and picks the
    latest ``SUCCESS`` one, else the latest ``IN_PROGRESS`` one. If there's neither, takes a
    snapshot of all indices named ``<dc>-<namespace>-pre-decommission-<YYYYMMDD>-<HHMM>`` (UTC,
    when it's taken) if the ``create_snapshot`` param is on, and fails if it's off.
  - ``wait_for_snapshot``: sensor that checks the picked snapshot every minute
    (``GET /_snapshot/es-snapshots/<snapshot>``), in reschedule mode so it doesn't hold a worker,
    until it's ``SUCCESS``. Fails if it ends in another state or takes longer than 6 hours.
  - ``verify_snapshot``: fails unless the snapshot (``GET /_snapshot/es-snapshots/<snapshot>``)
    includes every index ``list_indices`` found, listing the missing ones. A snapshot from earlier
    that day can miss indices created since, and one taken here skips unavailable indices.

- ``summarize``: logs every cluster's indices together, with counts.

``elasticsearch_clusters`` is a per-deployment JSON Variable listing the Elasticsearch clusters,
each with the DAGs that should run on it::

    [
        {
            "conn_id": "es-wordtags",
            "dc": "den",
            "namespace": "wordtags",
            "active_dags": ["decom_list_tenant_indices_dag"]
        }
    ]

Connections: each cluster's ``conn_id`` (Elasticsearch, HTTP) and ``tenant-registry`` (HTTP).
"""

import json
import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowSkipException
from airflow.models import Param, Variable
from airflow.operators.python import get_current_context
from airflow.sensors.base import PokeReturnValue

from seaas_indices import match_seaas_index
from utils import http_hook_get, http_hook_put, param_value

log = logging.getLogger(__name__)

TENANT_REGISTRY_CONN_ID = "tenant-registry"
TENANT_REGISTRY_EXPRESS_INSTANCES_ENDPOINT = "/api/v0/applications/id/com.medallia.express/instances/"
# Per-deployment JSON Variable with the clusters to look in; format in the module docstring.
CLUSTERS_VARIABLE = "elasticsearch_clusters"
CLUSTER_FIELDS = ("conn_id", "dc", "namespace", "active_dags")
SNAPSHOT_REPOSITORY = "es-snapshots"
SNAPSHOT_BODY = {"indices": "*", "ignore_unavailable": True, "include_global_state": False}
SNAPSHOT_POKE_INTERVAL_SECONDS = 60
SNAPSHOT_TIMEOUT_SECONDS = 6 * 60 * 60


def validate_clusters(clusters: Any, dag_id: str) -> List[Dict[str, str]]:
    """Return the ``CLUSTERS_VARIABLE`` clusters that have ``dag_id`` in ``active_dags``.

    Each as ``{"conn_id", "dc", "namespace"}``, sorted by ``conn_id``. Raises ``ValueError``
    unless the value is a non-empty list of objects that all have ``CLUSTER_FIELDS``, with
    ``active_dags`` a list, and at least one cluster, each with its own ``conn_id``, has ``dag_id``.
    """
    if not isinstance(clusters, list) or not clusters:
        raise ValueError(f"Variable {CLUSTERS_VARIABLE} must be a non-empty JSON list of clusters")
    for i, cluster in enumerate(clusters):
        missing = [field for field in CLUSTER_FIELDS if not isinstance(cluster, dict) or field not in cluster]
        if missing:
            raise ValueError(f"Variable {CLUSTERS_VARIABLE} entry {i} is missing {missing}")
        if not isinstance(cluster["active_dags"], list):
            raise ValueError(f"Variable {CLUSTERS_VARIABLE} entry {i}: active_dags must be a list of DAG ids")

    active = sorted(
        (
            {"conn_id": c["conn_id"], "dc": c["dc"], "namespace": c["namespace"]}
            for c in clusters
            if dag_id in c["active_dags"]
        ),
        key=lambda c: c["conn_id"],
    )
    if not active:
        raise ValueError(f"No cluster in Variable {CLUSTERS_VARIABLE} has {dag_id} in its active_dags")
    conn_ids = [c["conn_id"] for c in active]
    if len(set(conn_ids)) != len(conn_ids):
        raise ValueError(f"Clusters with {dag_id} active share a conn_id: {conn_ids}")
    log.info("Looking in %d Elasticsearch clusters: %s", len(active), active)
    return active


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


def snapshots_started_on(snapshots: List[Dict[str, Any]], day: date) -> Dict[str, List[str]]:
    """Group the ids of the snapshots in a ``GET /_cat/snapshots`` response that started on ``day`` (UTC) by status.

    E.g. ``{"SUCCESS": [...], "IN_PROGRESS": [...]}``, each list oldest first.
    """
    by_status = {}
    for snapshot in sorted(snapshots, key=lambda snapshot: int(snapshot["start_epoch"])):
        if datetime.fromtimestamp(int(snapshot["start_epoch"]), tz=timezone.utc).date() == day:
            by_status.setdefault(snapshot["status"], []).append(snapshot["id"])
    return by_status


def missing_from_snapshot(indices: List[str], snapshot_response: dict) -> List[str]:
    """Return the ``indices`` missing from a ``GET /_snapshot/<repository>/<snapshot>`` response's snapshot, sorted."""
    return sorted(set(indices) - set(snapshot_response["snapshots"][0]["indices"]))


def pre_decommission_snapshot_name(cluster: Dict[str, Any], now: datetime) -> str:
    """``<dc>-<namespace>-pre-decommission-<YYYYMMDD>-<HHMM>``, with ``now`` in UTC."""
    return f"{cluster['dc']}-{cluster['namespace']}-pre-decommission-{now.astimezone(timezone.utc):%Y%m%d-%H%M}"


@dag(
    dag_display_name="Decom: List Tenant Indices",
    tags=["spf", "elasticsearch", "decom"],
    description="List the seaas surveys and topic-builder indices of a decommissioned tenant.",
    doc_md=__doc__,
    max_active_runs=1,
    schedule=None,
    is_paused_upon_creation=True,
    catchup=False,
    params={
        "hostname": Param("", type="string", description="Hostname of the tenant's Express instance, e.g. acme.medallia.com"),
        "tenant": Param("", type="string", description="The tenant's name (its in_app_id), e.g. acme"),
        "create_snapshot": Param(
            False,
            type="boolean",
            description="Take a snapshot of all indices on clusters with the tenant's indices but no snapshot that day",
        ),
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
        dag_id = get_current_context()["dag"].dag_id
        return validate_clusters(Variable.get(CLUSTERS_VARIABLE, deserialize_json=True), dag_id)

    @task
    def summarize(registry_tenant: Dict[str, Any], per_cluster: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        lines = [f"{c['dc']} - {c['namespace']}: {len(c['indices'])} indices" for c in per_cluster]
        lines += [f"  {c['dc']} - {c['namespace']}: {index}" for c in per_cluster for index in c["indices"]]
        log.info(
            "%d indices for %s across %d clusters:\n%s",
            sum(len(c["indices"]) for c in per_cluster),
            registry_tenant,
            len(per_cluster),
            "\n".join(lines),
        )
        return {c["conn_id"]: c["indices"] for c in per_cluster}

    # The cluster group is mapped over the clusters; each of its tasks is labelled with the cluster's conn_id in the UI.
    @task_group(group_id="cluster")
    def per_cluster(cluster: Dict[str, str], registry_tenant: Dict[str, Any]):
        @task(map_index_template="{{ cluster_name }}")
        def list_indices(cluster: Dict[str, str], registry_tenant: Dict[str, Any]) -> Dict[str, Any]:
            get_current_context()["cluster_name"] = cluster["conn_id"]
            fetched = http_hook_get(cluster["conn_id"], "/_cat/indices", params={"h": "index", "format": "json"})
            indices = select_indices([r["index"] for r in fetched], registry_tenant)
            log.info("%d indices for %s on %s:\n%s", len(indices), registry_tenant, cluster, "\n".join(indices))
            return {**cluster, "indices": indices}

        @task(map_index_template="{{ cluster_name }}")
        def check_snapshot(cluster: Dict[str, Any]) -> Dict[str, Any]:
            context = get_current_context()
            conn_id = context["cluster_name"] = cluster["conn_id"]
            if not cluster["indices"]:
                raise AirflowSkipException(f"No indices of the tenant on {conn_id}")

            day = date.fromisoformat(context["ds"])
            snapshots = http_hook_get(conn_id, f"/_cat/snapshots/{SNAPSHOT_REPOSITORY}", params={"format": "json"})
            by_status = snapshots_started_on(snapshots, day)
            log.info("Snapshots in %s on %s started on %s: %s", SNAPSHOT_REPOSITORY, conn_id, day, by_status)
            picked = {
                "conn_id": conn_id,
                "dc": cluster["dc"],
                "namespace": cluster["namespace"],
                "indices": cluster["indices"],
            }

            for status in ("SUCCESS", "IN_PROGRESS"):
                if by_status.get(status):
                    log.info("Using %s snapshot %s", status, by_status[status][-1])
                    return {**picked, "snapshot": by_status[status][-1]}

            if not param_value("create_snapshot"):
                raise ValueError(
                    f"No successful or running snapshot in {SNAPSHOT_REPOSITORY} on {conn_id} started on {day}; "
                    "trigger with create_snapshot on to take one"
                )
            name = pre_decommission_snapshot_name(cluster, datetime.now(timezone.utc))
            log.info("Taking snapshot %s in %s on %s: %s", name, SNAPSHOT_REPOSITORY, conn_id, SNAPSHOT_BODY)
            response = http_hook_put(conn_id, f"_snapshot/{SNAPSHOT_REPOSITORY}/{name}", json.dumps(SNAPSHOT_BODY))
            log.info("Response:\n%s", json.dumps(response, indent=2))
            return {**picked, "snapshot": name}

        @task.sensor(
            map_index_template="{{ cluster_name }}",
            poke_interval=SNAPSHOT_POKE_INTERVAL_SECONDS,
            timeout=SNAPSHOT_TIMEOUT_SECONDS,
            mode="reschedule",
        )
        def wait_for_snapshot(picked: Dict[str, Any]) -> PokeReturnValue:
            conn_id = get_current_context()["cluster_name"] = picked["conn_id"]
            response = http_hook_get(conn_id, f"/_snapshot/{SNAPSHOT_REPOSITORY}/{picked['snapshot']}")
            state = response["snapshots"][0]["state"]
            log.info("Snapshot %s on %s: %s", picked["snapshot"], conn_id, state)
            if state not in ("SUCCESS", "IN_PROGRESS"):
                raise ValueError(f"Snapshot {picked['snapshot']} on {conn_id} ended {state}")
            return PokeReturnValue(is_done=state == "SUCCESS", xcom_value={**picked, "state": state})

        @task(map_index_template="{{ cluster_name }}")
        def verify_snapshot(waited: Dict[str, Any]) -> Dict[str, Any]:
            conn_id = get_current_context()["cluster_name"] = waited["conn_id"]
            response = http_hook_get(conn_id, f"/_snapshot/{SNAPSHOT_REPOSITORY}/{waited['snapshot']}")
            missing = missing_from_snapshot(waited["indices"], response)
            if missing:
                raise ValueError(
                    f"Snapshot {waited['snapshot']} on {conn_id} is missing {len(missing)} of the "
                    f"{len(waited['indices'])} indices to delete:\n" + "\n".join(missing)
                )
            log.info(
                "Snapshot %s on %s includes all %d indices to delete",
                waited["snapshot"],
                conn_id,
                len(waited["indices"]),
            )
            return waited

        listed = list_indices(cluster, registry_tenant)
        verify_snapshot(wait_for_snapshot(check_snapshot(listed)))
        return listed

    registry_tenant = fetch_tenant()
    listed = per_cluster.partial(registry_tenant=registry_tenant).expand(cluster=load_clusters())
    summarize(registry_tenant, listed)


decom_list_tenant_indices_dag()

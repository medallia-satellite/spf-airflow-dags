"""The Elasticsearch clusters a DAG runs on, from the ``elasticsearch_clusters`` Variable.

Each Airflow deployment (one per DC, all with the same DAGs) sets the Variable to its clusters.
Every DAG runs on every cluster with its default params, so a new DAG needs no Variable change; a
cluster's optional ``dags`` object only holds exceptions::

    [
        {"conn_id": "es-shared", "dc": "den", "namespace": "sharedservices-elasticsearch"},
        {
            "conn_id": "es-wordtags",
            "dc": "den",
            "namespace": "wordtags",
            "dags": {
                "reconcile_monthly_indices_dag": {"dry_run": false},
                "some_dag_not_here": false
            }
        }
    ]

``dags[<dag_id>]`` missing, ``null``, ``true`` or ``{}``: the DAG runs with its defaults; an object:
it runs with those params over its defaults; ``false``: it doesn't run on that cluster.

Kept out of the DAG files so it can be tested without Airflow.
"""

from typing import Any, Dict, List, Optional

CLUSTERS_VARIABLE = "elasticsearch_clusters"
CLUSTER_FIELDS = ("conn_id", "dc", "namespace")


def clusters_for(
    clusters: Any, dag_id: str, default_params: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """The ``CLUSTERS_VARIABLE`` clusters ``dag_id`` runs on, in order, as ``{"conn_id", "dc", "namespace", "params"}``.

    ``params`` is ``default_params`` with the cluster's ``dags[dag_id]`` object over it. Raises
    ``ValueError`` if the Variable is invalid, two clusters share a ``conn_id``, or ``dag_id`` is
    turned off on every cluster.
    """
    if not isinstance(clusters, list) or not clusters:
        raise ValueError(f"Variable {CLUSTERS_VARIABLE} must be a non-empty JSON list of clusters")

    selected = []
    for i, cluster in enumerate(clusters):
        if not isinstance(cluster, dict):
            raise ValueError(f"Variable {CLUSTERS_VARIABLE} entry {i} isn't a JSON object")
        missing = [field for field in CLUSTER_FIELDS if field not in cluster]
        if missing:
            raise ValueError(f"Variable {CLUSTERS_VARIABLE} entry {i} is missing {missing}")
        if "active_dags" in cluster:
            raise ValueError(
                f"Variable {CLUSTERS_VARIABLE} entry {i} has active_dags, which was replaced: DAGs now run on "
                "every cluster, and dags only holds exceptions ({dag_id: params, or false to turn it off})"
            )
        dags = cluster.get("dags", {})
        if not isinstance(dags, dict):
            raise ValueError(f"Variable {CLUSTERS_VARIABLE} entry {i}: dags must be a JSON object")
        override = dags.get(dag_id)
        if override is not None and not isinstance(override, (dict, bool)):
            raise ValueError(
                f"Variable {CLUSTERS_VARIABLE} entry {i}: dags.{dag_id} must be an object of params, "
                "null, true or false"
            )
        if override is False:
            continue
        params = {**(default_params or {}), **(override if isinstance(override, dict) else {})}
        selected.append({**{field: cluster[field] for field in CLUSTER_FIELDS}, "params": params})

    conn_ids = [c["conn_id"] for c in selected]
    duplicates = sorted({conn_id for conn_id in conn_ids if conn_ids.count(conn_id) > 1})
    if duplicates:
        raise ValueError(f"Variable {CLUSTERS_VARIABLE} has clusters sharing a conn_id: {duplicates}")
    if not selected:
        raise ValueError(f"{dag_id} is turned off on every cluster in Variable {CLUSTERS_VARIABLE}")
    return selected

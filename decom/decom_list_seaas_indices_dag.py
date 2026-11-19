"""Decom: list a company's seaas Elasticsearch indices.

Manually triggered with a ``company`` (its in_app_id) and its ``instance`` (hostname). On every
cluster of the ``elasticsearch_clusters`` Variable (see ``elasticsearch_clusters.py``), unless the
cluster turns this DAG off in its ``dags``, lists the indices matching
``seaas-<company>_*-<instance>-<company>-*``, e.g. for acme on acme.medallia.eu::

    seaas-acme_surveys-acme.medallia.eu-acme-0
    seaas-acme_topic-builder-acme.medallia.eu-acme-2023-10-01-123456-000045

``company`` and ``instance`` must be a plain in_app_id and hostname, so no wildcard reaches the
clusters. Tasks, one per cluster for the first two, each timing out after 5 minutes and retrying
twice:

- ``list_indices``: the matching indices with their status, document count and size.
- ``check_snapshots``: for the open indices with documents, the latest ``SUCCESS`` snapshot in the
  cluster's ``es-snapshots`` repository that started in the last 24 hours and includes the index.
  Indices without one, and clusters whose snapshots can't be read (e.g. no such repository), are
  warned about, not failed on.
- ``summarize``: logs the indices, one block per namespace, skipping clusters without any::

    namespace: sharedservices-elasticsearch (3 indices, 28,401 docs, 1.2gb, 1 closed, 1 without snapshot)
    seaas-acme_surveys-acme.medallia.eu-acme-0 (28,401 docs, 1.1gb, no snapshot in 24h)
    seaas-acme_topic-builder-acme.medallia.eu-acme-2023-10-01-123456-000045 (0 docs, 208b)
    seaas-acme_topic-builder-acme.medallia.eu-acme-2023-11-01-123456-000046 (closed, 95.3mb)

  and returns ``{conn_id: {index: {"status", "docs", "bytes"[, "snapshot"]}}}`` for those
  clusters; ``snapshot`` is the snapshot's name, or None if there's none in 24h. Docs only count
  open indices. The logic lives in ``decom.indices``. It doesn't change anything.
``elasticsearch_clusters`` looks like this; the ``dags`` entry is only needed to skip a cluster::

    [
        {"conn_id": "es-shared", "dc": "den", "namespace": "sharedservices-elasticsearch"},
        {
            "conn_id": "es-wordtags-qa",
            "dc": "den",
            "namespace": "wordtags-qa",
            "dags": {"decom_list_seaas_indices_dag": false}
        }
    ]
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.models import Param, Variable
from airflow.operators.python import get_current_context

from decom.indices import (
    CAT_INDICES_COLUMNS,
    COMPANY_PATTERN,
    INSTANCE_PATTERN,
    SNAPSHOT_FILTER_PATH,
    SNAPSHOT_REPOSITORY,
    SNAPSHOT_WINDOW,
    format_cluster,
    format_summary,
    index_pattern,
    indices_with_docs,
    parse_indices,
    recent_snapshots,
)
from elasticsearch_clusters import CLUSTERS_VARIABLE, clusters_for
from utils import http_hook_get, param_value

log = logging.getLogger(__name__)

# Per-cluster tasks: one mapped instance per cluster, labelled with its conn_id in the UI.
PER_CLUSTER_TASK = dict(
    map_index_template="{{ conn_id }}",
    retries=2,
    retry_delay=timedelta(minutes=5),
    execution_timeout=timedelta(minutes=5),
)


@dag(
    dag_display_name="Decom: List SEAAS Indices",
    tags=["spf", "elasticsearch", "decom"],
    description="List a company's seaas indices on every active Elasticsearch cluster.",
    doc_md=__doc__,
    max_active_runs=1,
    schedule=None,
    is_paused_upon_creation=True,
    catchup=False,
    params={
        "company": Param("", type="string", pattern=COMPANY_PATTERN, description="The company's in_app_id, e.g. acme"),
        "instance": Param(
            "", type="string", pattern=INSTANCE_PATTERN, description="The instance hostname, e.g. acme.medallia.eu"
        ),
    },
    render_template_as_native_obj=True,
)
def decom_list_seaas_indices_dag():
    @task
    def load_clusters() -> List[Dict[str, Any]]:
        dag_id = get_current_context()["dag"].dag_id
        return clusters_for(Variable.get(CLUSTERS_VARIABLE, deserialize_json=True), dag_id)

    @task(**PER_CLUSTER_TASK)
    def list_indices(cluster: Dict[str, Any]) -> Dict[str, Any]:
        conn_id = get_current_context()["conn_id"] = cluster["conn_id"]
        pattern = index_pattern(param_value("company"), param_value("instance"))
        fetched = http_hook_get(
            conn_id, f"/_cat/indices/{pattern}", params={"h": CAT_INDICES_COLUMNS, "bytes": "b", "format": "json"}
        )
        indices = parse_indices(fetched)
        log.info("Indices matching %s on %s:\n%s", pattern, conn_id, format_cluster(cluster["namespace"], indices))
        return {**cluster, "indices": indices}

    @task(**PER_CLUSTER_TASK)
    def check_snapshots(cluster: Dict[str, Any]) -> Dict[str, Any]:
        conn_id = get_current_context()["conn_id"] = cluster["conn_id"]
        to_check = indices_with_docs(cluster["indices"])
        if not to_check:
            log.info("No indices with documents on %s, nothing to check", conn_id)
            return cluster

        since = datetime.now(timezone.utc) - SNAPSHOT_WINDOW
        try:
            response = http_hook_get(
                conn_id, f"/_snapshot/{SNAPSHOT_REPOSITORY}/_all", params={"filter_path": SNAPSHOT_FILTER_PATH}
            )
        except Exception as error:  # e.g. no such repository on this cluster: warn, don't fail the run
            log.warning(
                "Couldn't get the %s snapshots on %s, not checking them: %s", SNAPSHOT_REPOSITORY, conn_id, error
            )
            return cluster

        snapshots = recent_snapshots(response, since)
        indices = {
            index: {**i, "snapshot": snapshots.get(index)} if index in to_check else i
            for index, i in cluster["indices"].items()
        }
        missing = [index for index in to_check if not snapshots.get(index)]
        log.log(
            logging.WARNING if missing else logging.INFO,
            "%d of %d indices with documents on %s have a %s snapshot since %s; without one:\n%s",
            len(to_check) - len(missing),
            len(to_check),
            conn_id,
            SNAPSHOT_REPOSITORY,
            since.isoformat(timespec="seconds"),
            "\n".join(missing) or "none",
        )
        return {**cluster, "indices": indices}

    @task
    def summarize(per_cluster: List[Dict[str, Any]]) -> Dict[str, Dict[str, Dict[str, Any]]]:
        log.info(
            "Indices of %s on %s:\n\n%s", param_value("company"), param_value("instance"), format_summary(per_cluster)
        )
        return {c["conn_id"]: c["indices"] for c in per_cluster if c["indices"]}

    listed = list_indices.expand(cluster=load_clusters())
    summarize(check_snapshots.expand(cluster=listed))


decom_list_seaas_indices_dag()

"""Decom: list a company's seaas Elasticsearch indices.

Manually triggered with a ``company`` (its in_app_id) and its ``instance`` (hostname). On every
cluster of the ``elasticsearch_clusters`` Variable (see ``elasticsearch_clusters.py``), unless the
cluster turns this DAG off in its ``dags``, lists the indices matching
``seaas-<company>_*-<instance>-<company>-*``, e.g. for acme on acme.medallia.eu::

    seaas-acme_surveys-acme.medallia.eu-acme-0
    seaas-acme_topic-builder-acme.medallia.eu-acme-2023-10-01-123456-000045

``company`` and ``instance`` must be a plain in_app_id and hostname, so no wildcard reaches the
clusters. Each cluster is listed by its own task, which times out after 5 minutes and retries
twice. A final ``summarize`` task logs the indices with their document counts and sizes, one
block per namespace, skipping clusters without any::

    namespace: sharedservices-elasticsearch (3 indices, 28,401 docs, 1.2gb, 1 closed)
    seaas-acme_surveys-acme.medallia.eu-acme-0 (28,401 docs, 1.1gb)
    seaas-acme_topic-builder-acme.medallia.eu-acme-2023-10-01-123456-000045 (0 docs, 208b)
    seaas-acme_topic-builder-acme.medallia.eu-acme-2023-11-01-123456-000046 (closed, 95.3mb)

and returns ``{conn_id: {index: {"status", "docs", "bytes"}}}`` for those clusters. Docs only
count open indices. The logic lives in ``decom.indices``. It doesn't change anything.
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
from datetime import timedelta
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.models import Param, Variable
from airflow.operators.python import get_current_context

from decom.indices import (
    CAT_INDICES_COLUMNS,
    COMPANY_PATTERN,
    INSTANCE_PATTERN,
    format_cluster,
    format_summary,
    index_pattern,
    parse_indices,
)
from elasticsearch_clusters import CLUSTERS_VARIABLE, clusters_for
from utils import http_hook_get, param_value

log = logging.getLogger(__name__)


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

    # One mapped instance per cluster, labelled with its conn_id in the UI.
    @task(
        map_index_template="{{ conn_id }}",
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=5),
    )
    def list_indices(cluster: Dict[str, Any]) -> Dict[str, Any]:
        conn_id = get_current_context()["conn_id"] = cluster["conn_id"]
        pattern = index_pattern(param_value("company"), param_value("instance"))
        fetched = http_hook_get(
            conn_id, f"/_cat/indices/{pattern}", params={"h": CAT_INDICES_COLUMNS, "bytes": "b", "format": "json"}
        )
        indices = parse_indices(fetched)
        log.info("Indices matching %s on %s:\n%s", pattern, conn_id, format_cluster(cluster["namespace"], indices))
        return {**cluster, "indices": indices}

    @task
    def summarize(per_cluster: List[Dict[str, Any]]) -> Dict[str, Dict[str, Dict[str, Any]]]:
        log.info(
            "Indices of %s on %s:\n\n%s", param_value("company"), param_value("instance"), format_summary(per_cluster)
        )
        return {c["conn_id"]: c["indices"] for c in per_cluster if c["indices"]}

    summarize(list_indices.expand(cluster=load_clusters()))


decom_list_seaas_indices_dag()

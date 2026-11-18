"""Behaviour of the ``decom.indices`` helpers behind ``decom_list_seaas_indices_dag``: which input
reaches the clusters, which clusters it asks, and how it reads their answer. The summary is only
log text, so it isn't pinned here."""

import re

import pytest

from decom.indices import COMPANY_PATTERN, INSTANCE_PATTERN, active_clusters, index_pattern, parse_indices

DAG_ID = "decom_list_seaas_indices_dag"

SURVEYS = "seaas-acme_surveys-acme.medallia.eu-acme-0"
TOPIC_BUILDER = "seaas-acme_topic-builder-acme.medallia.eu-acme-2023-10-01-123456-000045"


class TestIndexPattern:
    def test_matches_the_companys_indices_on_its_instance(self):
        assert index_pattern("acme", "acme.medallia.eu") == "seaas-acme_*-acme.medallia.eu-acme-*"

    @pytest.mark.parametrize("company", ["acme", "acme_eu", "acme2"])
    def test_accepts_in_app_ids(self, company):
        assert re.fullmatch(COMPANY_PATTERN, company)
        index_pattern(company, "acme.medallia.eu")

    @pytest.mark.parametrize("instance", ["acme.medallia.eu", "acme-eu.sub.example.io", "localhost"])
    def test_accepts_hostnames(self, instance):
        assert re.fullmatch(INSTANCE_PATTERN, instance)
        index_pattern("acme", instance)

    # Each of these would widen the request to other companies' indices, or break it.
    @pytest.mark.parametrize("company", ["", None, "*", "acme*", "acme,beta", "ac me", "acme-eu", "acme/x"])
    def test_rejects_other_companies(self, company):
        assert not re.fullmatch(COMPANY_PATTERN, company or "")
        with pytest.raises(ValueError, match="company"):
            index_pattern(company, "acme.medallia.eu")

    @pytest.mark.parametrize("instance", ["", None, "*", "acme.medallia.*", "acme..medallia.eu", "a,b", "a/b", "a b"])
    def test_rejects_other_instances(self, instance):
        assert not re.fullmatch(INSTANCE_PATTERN, instance or "")
        with pytest.raises(ValueError, match="instance"):
            index_pattern("acme", instance)


def cluster(conn_id, active_dags=(DAG_ID,), dc="den", namespace="ns"):
    """One ``elasticsearch_clusters`` entry."""
    return {"conn_id": conn_id, "dc": dc, "namespace": namespace, "active_dags": list(active_dags)}


class TestActiveClusters:
    def test_keeps_the_clusters_with_this_dag_active_in_order(self):
        clusters = [
            cluster("es-wordtags", dc="sea", namespace="wordtags"),
            cluster("es-other", active_dags=["reconcile_monthly_indices_dag"]),
            cluster("es-none", active_dags=[]),
            {"conn_id": "es-no-dags", "dc": "den", "namespace": "ns"},
            cluster("es-shared", active_dags=["reconcile_monthly_indices_dag", DAG_ID], namespace="shared"),
        ]

        assert active_clusters(clusters, DAG_ID) == [
            {"conn_id": "es-wordtags", "dc": "sea", "namespace": "wordtags"},
            {"conn_id": "es-shared", "dc": "den", "namespace": "shared"},
        ]

    @pytest.mark.parametrize("clusters", [[], [cluster("es-a", active_dags=["other_dag"])]])
    def test_fails_when_no_cluster_has_this_dag_active(self, clusters):
        with pytest.raises(ValueError, match=f"No cluster .* has {DAG_ID}"):
            active_clusters(clusters, DAG_ID)


class TestParseIndices:
    def test_reads_status_docs_and_bytes_sorted_by_index(self):
        response = [
            {"index": TOPIC_BUILDER, "status": "open", "docs.count": "0", "store.size": "208"},
            {"index": SURVEYS, "status": "open", "docs.count": "28401", "store.size": "1181116006"},
        ]

        assert list(parse_indices(response).items()) == [
            (SURVEYS, {"status": "open", "docs": 28401, "bytes": 1181116006}),
            (TOPIC_BUILDER, {"status": "open", "docs": 0, "bytes": 208}),
        ]

    def test_closed_indices_have_no_doc_count(self):
        response = [{"index": SURVEYS, "status": "close", "docs.count": None, "store.size": None}]

        assert parse_indices(response) == {SURVEYS: {"status": "close", "docs": None, "bytes": 0}}

    def test_handles_no_indices(self):
        assert parse_indices([]) == {}

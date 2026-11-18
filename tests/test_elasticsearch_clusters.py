"""Behaviour of ``clusters_for``: every DAG runs on every cluster of the ``elasticsearch_clusters``
Variable unless the cluster's ``dags`` turns it off, with the cluster's params over the DAG's
defaults. A mistake here runs a DAG on a cluster it shouldn't touch, or with the wrong params."""

import pytest

from elasticsearch_clusters import clusters_for

DAG_ID = "some_dag"


def cluster(conn_id, dags=None, dc="den", namespace="ns"):
    """One ``elasticsearch_clusters`` entry, with ``dags`` only if given."""
    entry = {"conn_id": conn_id, "dc": dc, "namespace": namespace}
    if dags is not None:
        entry["dags"] = dags
    return entry


def conn_ids(selected):
    return [c["conn_id"] for c in selected]


class TestSelection:
    def test_runs_on_every_cluster_by_default_in_order(self):
        clusters = [cluster("es-b", dc="sea", namespace="wordtags"), cluster("es-a")]

        assert clusters_for(clusters, DAG_ID) == [
            {"conn_id": "es-b", "dc": "sea", "namespace": "wordtags", "params": {}},
            {"conn_id": "es-a", "dc": "den", "namespace": "ns", "params": {}},
        ]

    def test_false_turns_the_dag_off_on_that_cluster_only(self):
        clusters = [cluster("es-a", {DAG_ID: False}), cluster("es-b"), cluster("es-c", {"other_dag": False})]

        assert conn_ids(clusters_for(clusters, DAG_ID)) == ["es-b", "es-c"]
        assert conn_ids(clusters_for(clusters, "other_dag")) == ["es-a", "es-b"]

    def test_fails_when_the_dag_is_off_everywhere(self):
        with pytest.raises(ValueError, match=f"{DAG_ID} is turned off on every cluster"):
            clusters_for([cluster("es-a", {DAG_ID: False})], DAG_ID)


class TestParams:
    @pytest.mark.parametrize("override", [None, True, {}])
    def test_runs_with_the_defaults_when_the_cluster_gives_no_params(self, override):
        selected = clusters_for([cluster("es-a", {DAG_ID: override})], DAG_ID, {"dry_run": True})

        assert selected[0]["params"] == {"dry_run": True}

    def test_cluster_params_go_over_the_defaults(self):
        clusters = [cluster("es-a", {DAG_ID: {"dry_run": False, "extra": 1}}), cluster("es-b")]

        selected = clusters_for(clusters, DAG_ID, {"dry_run": True, "keep": "x"})

        assert [c["params"] for c in selected] == [
            {"dry_run": False, "keep": "x", "extra": 1},
            {"dry_run": True, "keep": "x"},
        ]

    def test_other_dags_params_dont_apply(self):
        selected = clusters_for([cluster("es-a", {"other_dag": {"dry_run": False}})], DAG_ID, {"dry_run": True})

        assert selected[0]["params"] == {"dry_run": True}

    def test_defaults_are_not_shared_between_clusters(self):
        defaults = {"dry_run": True}

        selected = clusters_for([cluster("es-a", {DAG_ID: {"dry_run": False}}), cluster("es-b")], DAG_ID, defaults)

        assert defaults == {"dry_run": True}
        assert selected[1]["params"] == {"dry_run": True}


@pytest.mark.parametrize(
    "clusters, error",
    [
        (None, "non-empty JSON list"),
        ([], "non-empty JSON list"),
        ({"es-a": {"conn_id": "es-a"}}, "non-empty JSON list"),
        ([cluster("es-a"), "es-b"], "entry 1 isn't a JSON object"),
        ([{"conn_id": "es-a", "dc": "den"}], r"entry 0 is missing \['namespace'\]"),
        ([{**cluster("es-a"), "active_dags": [DAG_ID]}], "entry 0 has active_dags, which was replaced"),
        ([cluster("es-a", [DAG_ID])], "entry 0: dags must be a JSON object"),
        ([cluster("es-a", {DAG_ID: "off"})], f"entry 0: dags.{DAG_ID} must be an object of params"),
        ([cluster("es-a"), cluster("es-b"), cluster("es-a", dc="sea")], r"sharing a conn_id: \['es-a'\]"),
    ],
)
def test_rejects_invalid_variables(clusters, error):
    with pytest.raises(ValueError, match=error):
        clusters_for(clusters, DAG_ID)

"""Behaviour of the ``decom.indices`` helpers behind ``decom_list_seaas_indices_dag``: which input
reaches the clusters, how it reads their answer, and which snapshots count for an index. Which
clusters it asks is covered in
``test_elasticsearch_clusters.py``; the summary is only log text, so it isn't pinned here."""

import re
from datetime import datetime, timedelta, timezone

import pytest

from decom.indices import (
    COMPANY_PATTERN,
    INSTANCE_PATTERN,
    index_pattern,
    indices_with_docs,
    parse_indices,
    recent_snapshots,
)

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


class TestParseIndices:
    def test_reads_docs_and_bytes_sorted_by_index(self):
        response = [
            {"index": TOPIC_BUILDER, "docs.count": "0", "store.size": "208"},
            {"index": SURVEYS, "docs.count": "28401", "store.size": "1181116006"},
        ]

        assert list(parse_indices(response).items()) == [
            (SURVEYS, {"docs": 28401, "bytes": 1181116006}),
            (TOPIC_BUILDER, {"docs": 0, "bytes": 208}),
        ]

    def test_null_counts_of_closed_indices_are_zero(self):
        response = [{"index": SURVEYS, "docs.count": None, "store.size": None}]

        assert parse_indices(response) == {SURVEYS: {"docs": 0, "bytes": 0}}

    def test_handles_no_indices(self):
        assert parse_indices([]) == {}


def test_indices_with_docs_skips_empty_and_closed_indices():
    indices = {
        "with-docs": {"docs": 5, "bytes": 100},
        "empty": {"docs": 0, "bytes": 208},
        "closed": parse_indices([{"index": "closed", "docs.count": None, "store.size": None}])["closed"],
    }

    assert indices_with_docs(indices) == ["with-docs"]


NOW = datetime(2026, 11, 19, 12, 0, tzinfo=timezone.utc)
SINCE = NOW - timedelta(hours=24)


def snapshot(name, started, indices, state="SUCCESS"):
    """One snapshot of a ``GET /_snapshot/<repository>/_all`` response."""
    started_ms = int(started.timestamp() * 1000)
    return {"snapshot": name, "state": state, "start_time_in_millis": started_ms, "indices": indices}


class TestRecentSnapshots:
    def test_maps_each_index_to_its_latest_successful_snapshot_in_the_window(self):
        response = {
            "snapshots": [
                snapshot("late", NOW - timedelta(hours=1), [SURVEYS]),
                snapshot("early", NOW - timedelta(hours=20), [SURVEYS, TOPIC_BUILDER]),
            ]
        }

        assert recent_snapshots(response, SINCE) == {SURVEYS: "late", TOPIC_BUILDER: "early"}

    def test_counts_a_snapshot_started_exactly_24h_ago(self):
        response = {"snapshots": [snapshot("edge", SINCE, [SURVEYS])]}

        assert recent_snapshots(response, SINCE) == {SURVEYS: "edge"}

    @pytest.mark.parametrize(
        "older_or_failed",
        [
            snapshot("too-old", SINCE - timedelta(seconds=1), [SURVEYS]),
            snapshot("partial", NOW, [SURVEYS], state="PARTIAL"),
            snapshot("failed", NOW, [SURVEYS], state="FAILED"),
            snapshot("running", NOW, [SURVEYS], state="IN_PROGRESS"),
        ],
    )
    def test_ignores_older_and_unsuccessful_snapshots(self, older_or_failed):
        assert recent_snapshots({"snapshots": [older_or_failed]}, SINCE) == {}

    def test_handles_an_empty_repository(self):
        # With filter_path, an empty repository answers {} instead of {"snapshots": []}.
        assert recent_snapshots({}, SINCE) == {}

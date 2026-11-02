"""Behaviour of ``build_clickhouse_rows``: given a day's cluster totals and tenant usage, the
``tenant_resource_usage_daily`` rows it produces. Rows are compared by column name and keyed by
(resource_type, tenant_id), so neither the column order nor the row order is pinned, only that
each value lands in the right column."""

import logging
from datetime import date, datetime, timezone

import pytest

from cost_obs.rungs import UNATTRIBUTED_TENANT_ID
from cost_obs.tenant_rows import CLICKHOUSE_COLUMNS, build_clickhouse_rows

UNKNOWN = UNATTRIBUTED_TENANT_ID
DAY = date(2026, 10, 7)
CALCULATED_AT = datetime(2026, 10, 8, 6, 0, 5, tzinfo=timezone.utc)

CLUSTER_TOTALS = {"cpu_core_hours": 100.0, "cpu_request_core_hours": 200.0, "memory_gib": 50.0, "memory_request_gib": 80.0}
TENANT_USAGE = [
    {"tenant_id": UNKNOWN, "cpu_ms": 0, "memory_gib": 0.0, "storage_gib": 1.0, "snapshots": 24},
    {"tenant_id": 101, "cpu_ms": 1, "memory_gib": 2.0, "storage_gib": 3.0, "snapshots": 24},
    {"tenant_id": 102, "cpu_ms": 3, "memory_gib": 8.0, "storage_gib": 6.0, "snapshots": 24},
]


def build(cluster_totals=CLUSTER_TOTALS, tenant_usage=TENANT_USAGE):
    """``build_clickhouse_rows`` output as ``{(resource_type, tenant_id): {column: value}}``."""
    rows = build_clickhouse_rows(cluster_totals, tenant_usage, DAY, "den", "sharedservices-elasticsearch", 3, CALCULATED_AT)
    by_key = {}
    for row in rows:
        named = dict(zip(CLICKHOUSE_COLUMNS, row))
        key = (named["resource_type"], named["tenant_id"])
        assert key not in by_key, f"duplicate row for {key}"
        by_key[key] = named
    assert len(by_key) == len(rows)
    return by_key


def resource(rows, resource_type):
    """``{tenant_id: row}`` for one resource type."""
    return {tenant_id: row for (rt, tenant_id), row in rows.items() if rt == resource_type}


def test_writes_every_column_of_the_table():
    assert sorted(CLICKHOUSE_COLUMNS) == sorted(
        ["date", "dc", "service", "deployment", "tenant_id", "resource_type", "value", "unit", "weight",
         "cluster_total", "rung", "state", "revision", "calculated_at"]
    )
    for row in build_clickhouse_rows(CLUSTER_TOTALS, TENANT_USAGE, DAY, "den", "ns", 1, CALCULATED_AT):
        assert len(row) == len(CLICKHOUSE_COLUMNS)


def test_one_row_per_tenant_and_resource_type():
    assert set(build()) == {
        (resource_type, tenant_id)
        for resource_type in ["cpu", "cpu_request", "memory", "memory_request", "storage"]
        for tenant_id in [UNKNOWN, 101, 102]
    }


def test_run_fields_are_the_same_on_every_row():
    for row in build().values():
        assert row["date"] == DAY
        assert row["dc"] == "den"
        assert row["service"] == "elasticsearch"
        assert row["deployment"] == "sharedservices-elasticsearch"
        assert row["state"] == "provisional"
        assert row["revision"] == 3
        assert row["calculated_at"] == CALCULATED_AT


@pytest.mark.parametrize(
    "resource_type, unit, rung, cluster_total, values",
    [
        # cluster total split by cpu_ms (0:1:3).
        ("cpu", "core_hours", "activity", 100.0, {UNKNOWN: 0.0, 101: 25.0, 102: 75.0}),
        ("cpu_request", "core_hours", "activity", 200.0, {UNKNOWN: 0.0, 101: 50.0, 102: 150.0}),
        # measured memory first, the rest (50 - 10 = 40) by cpu_ms.
        ("memory", "gib_avg", "activity", 50.0, {UNKNOWN: 0.0, 101: 12.0, 102: 38.0}),
        # measured memory first, the rest (80 - 10 = 70) by cpu_ms.
        ("memory_request", "gib_avg", "activity", 80.0, {UNKNOWN: 0.0, 101: 19.5, 102: 60.5}),
        # each tenant's own storage; the total is their sum.
        ("storage", "gib_avg", "measured", 10.0, {UNKNOWN: 1.0, 101: 3.0, 102: 6.0}),
    ],
)
def test_resource_split(resource_type, unit, rung, cluster_total, values):
    rows = resource(build(), resource_type)
    assert {tenant_id: row["value"] for tenant_id, row in rows.items()} == pytest.approx(values)
    for tenant_id, row in rows.items():
        assert row["unit"] == unit
        assert row["rung"] == rung
        assert row["cluster_total"] == pytest.approx(cluster_total)
        assert row["weight"] == pytest.approx(values[tenant_id] / cluster_total)
    assert sum(row["weight"] for row in rows.values()) == pytest.approx(1.0)


def test_no_activity_falls_back_to_roster_except_storage():
    usage = [{**tenant, "cpu_ms": 0} for tenant in TENANT_USAGE]
    rows = build(tenant_usage=usage)
    assert {row["rung"] for (rt, _), row in rows.items() if rt != "storage"} == {"roster"}
    assert {row["rung"] for row in resource(rows, "storage").values()} == {"measured"}
    # Equal split between the known tenants; tenant 0 gets nothing.
    assert {t: row["value"] for t, row in resource(rows, "cpu").items()} == pytest.approx({UNKNOWN: 0.0, 101: 50.0, 102: 50.0})
    # 50 - 10 measured = 40 left, 20 each on top of the measured memory.
    assert {t: row["value"] for t, row in resource(rows, "memory").items()} == pytest.approx({UNKNOWN: 0.0, 101: 22.0, 102: 28.0})


def test_zero_cluster_total_gives_zero_weight():
    usage = [{**tenant, "storage_gib": 0.0} for tenant in TENANT_USAGE]
    for row in resource(build(tenant_usage=usage), "storage").values():
        assert row["cluster_total"] == 0.0
        assert row["value"] == 0.0
        assert row["weight"] == 0.0


def test_measured_memory_above_cluster_memory_keeps_measured_values(caplog):
    # Tenants' measured memory (4 + 6 = 10) is more than the containers' memory (5): nothing is left
    # to split, each tenant keeps its measured memory and the weights add up to more than 1.
    cluster_totals = {**CLUSTER_TOTALS, "memory_gib": 5.0}
    usage = [
        {**TENANT_USAGE[0], "memory_gib": 0.0},
        {**TENANT_USAGE[1], "memory_gib": 4.0},
        {**TENANT_USAGE[2], "memory_gib": 6.0},
    ]
    with caplog.at_level(logging.WARNING):
        rows = build(cluster_totals, usage)

    memory = resource(rows, "memory")
    assert {t: row["value"] for t, row in memory.items()} == pytest.approx({UNKNOWN: 0.0, 101: 4.0, 102: 6.0})
    assert {t: row["weight"] for t, row in memory.items()} == pytest.approx({UNKNOWN: 0.0, 101: 0.8, 102: 1.2})
    assert {row["cluster_total"] for row in memory.values()} == {5.0}
    assert {row["rung"] for row in memory.values()} == {"activity"}
    assert any(record.levelno == logging.WARNING for record in caplog.records)

    # memory_request (80) is still above the measured memory, so it splits as usual: 70 left by cpu_ms.
    memory_request = resource(rows, "memory_request")
    assert {t: row["value"] for t, row in memory_request.items()} == pytest.approx({UNKNOWN: 0.0, 101: 21.5, 102: 58.5})
    assert sum(row["weight"] for row in memory_request.values()) == pytest.approx(1.0)

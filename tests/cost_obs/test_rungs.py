"""Behaviour of the Cost Observability rung functions: given cluster totals and tenant usage, the
expected cluster total, per-tenant values and rung. Only inputs and outputs are checked, so the
implementation can change freely as long as these keep passing."""

import pytest

from cost_obs.rungs import UNATTRIBUTED_TENANT_ID, compute_weights, rung_activity, rung_footprint, rung_measured

UNKNOWN = UNATTRIBUTED_TENANT_ID


def tenants(*rows):
    """``(tenant_id, {column: value})`` pairs as ``tenant_usage`` dicts."""
    return [{"tenant_id": tenant_id, **columns} for tenant_id, columns in rows]


def assert_split(result, cluster_total, values, rung):
    got_total, got_values, got_rung = result
    assert got_total == pytest.approx(cluster_total)
    assert got_values == pytest.approx(values)
    assert got_rung == rung


class TestComputeWeights:
    def test_shares_follow_usage_in_tenant_order(self):
        usage = tenants((101, {"cpu_ms": 1}), (102, {"cpu_ms": 3}), (103, {"cpu_ms": 0}))
        weights, rung = compute_weights(usage, "cpu_ms", "activity")
        assert weights == pytest.approx([0.25, 0.75, 0.0])
        assert rung == "activity"

    def test_unattributed_usage_gets_its_share(self):
        usage = tenants((UNKNOWN, {"cpu_ms": 1}), (101, {"cpu_ms": 1}))
        assert compute_weights(usage, "cpu_ms", "activity") == (pytest.approx([0.5, 0.5]), "activity")

    def test_single_tenant_gets_everything(self):
        assert compute_weights(tenants((101, {"cpu_ms": 42})), "cpu_ms", "activity") == ([1.0], "activity")

    def test_no_usage_splits_equally_between_known_tenants(self):
        usage = tenants((UNKNOWN, {"cpu_ms": 0}), (101, {"cpu_ms": 0}), (102, {"cpu_ms": 0}), (103, {"cpu_ms": 0}))
        weights, rung = compute_weights(usage, "cpu_ms", "activity")
        assert weights == pytest.approx([0.0, 1 / 3, 1 / 3, 1 / 3])
        assert rung == "roster"

    def test_no_usage_and_no_known_tenant_is_unattributed(self):
        assert compute_weights(tenants((UNKNOWN, {"cpu_ms": 0})), "cpu_ms", "activity") == ([1.0], "unattributed")

    def test_no_tenants(self):
        assert compute_weights([], "cpu_ms", "activity") == ([], "unattributed")


class TestRungMeasured:
    def test_each_tenant_gets_its_own_usage_and_total_is_their_sum(self):
        usage = tenants((UNKNOWN, {"storage_gib": 0.5}), (101, {"storage_gib": 2.0}), (102, {"storage_gib": 7.5}))
        # The cluster totals are not used: the total comes from the tenants.
        assert_split(rung_measured("storage_gib")({"storage_gib": 999.0}, usage), 10.0, [0.5, 2.0, 7.5], "measured")

    def test_no_usage_stays_measured(self):
        usage = tenants((101, {"storage_gib": 0.0}), (102, {"storage_gib": 0.0}))
        assert_split(rung_measured("storage_gib")({}, usage), 0.0, [0.0, 0.0], "measured")

    def test_no_tenants(self):
        assert_split(rung_measured("storage_gib")({}, []), 0.0, [], "measured")


class TestRungActivity:
    def test_total_split_by_activity(self):
        usage = tenants((101, {"cpu_ms": 1}), (102, {"cpu_ms": 3}), (103, {"cpu_ms": 0}))
        assert_split(rung_activity("cpu_core_hours", "cpu_ms")({"cpu_core_hours": 100.0}, usage), 100.0, [25.0, 75.0, 0.0], "activity")

    def test_reads_only_its_total(self):
        usage = tenants((101, {"cpu_ms": 1}), (102, {"cpu_ms": 1}))
        cluster_totals = {"cpu_core_hours": 10.0, "cpu_request_core_hours": 40.0}
        assert_split(rung_activity("cpu_request_core_hours", "cpu_ms")(cluster_totals, usage), 40.0, [20.0, 20.0], "activity")

    def test_measured_usage_first_then_rest_by_activity(self):
        usage = tenants((101, {"memory_gib": 10.0, "cpu_ms": 1}), (102, {"memory_gib": 20.0, "cpu_ms": 3}))
        split = rung_activity("memory_gib", "cpu_ms", measured_key="memory_gib")
        # 100 - (10 + 20) = 70 left: 101 gets 10 + 70 * 1/4, 102 gets 20 + 70 * 3/4.
        assert_split(split({"memory_gib": 100.0}, usage), 100.0, [27.5, 72.5], "activity")

    def test_measured_above_total_leaves_nothing_to_split(self):
        usage = tenants((101, {"memory_gib": 60.0, "cpu_ms": 1}), (102, {"memory_gib": 50.0, "cpu_ms": 3}))
        split = rung_activity("memory_gib", "cpu_ms", measured_key="memory_gib")
        # Each tenant keeps its measured usage; the reported cluster total is still the cluster's.
        assert_split(split({"memory_gib": 100.0}, usage), 100.0, [60.0, 50.0], "activity")

    def test_no_activity_falls_back_to_roster(self):
        usage = tenants((UNKNOWN, {"cpu_ms": 0}), (101, {"cpu_ms": 0}), (102, {"cpu_ms": 0}))
        assert_split(rung_activity("cpu_core_hours", "cpu_ms")({"cpu_core_hours": 90.0}, usage), 90.0, [0.0, 45.0, 45.0], "roster")

    def test_no_activity_with_measured_splits_rest_equally(self):
        usage = tenants((101, {"memory_gib": 10.0, "cpu_ms": 0}), (102, {"memory_gib": 0.0, "cpu_ms": 0}))
        split = rung_activity("memory_gib", "cpu_ms", measured_key="memory_gib")
        assert_split(split({"memory_gib": 30.0}, usage), 30.0, [20.0, 10.0], "roster")

    def test_no_activity_and_no_known_tenant_is_unattributed(self):
        usage = tenants((UNKNOWN, {"cpu_ms": 0}))
        assert_split(rung_activity("cpu_core_hours", "cpu_ms")({"cpu_core_hours": 12.0}, usage), 12.0, [12.0], "unattributed")

    @pytest.mark.parametrize(
        "measured_key, rows",
        [
            (None, [(UNKNOWN, {"cpu_ms": 7}), (101, {"cpu_ms": 13}), (102, {"cpu_ms": 80})]),
            (None, [(UNKNOWN, {"cpu_ms": 0}), (101, {"cpu_ms": 0})]),
            ("memory_gib", [(101, {"memory_gib": 3.0, "cpu_ms": 5}), (102, {"memory_gib": 1.5, "cpu_ms": 0})]),
        ],
    )
    def test_tenant_values_add_up_to_cluster_total(self, measured_key, rows):
        split = rung_activity("total", "cpu_ms", measured_key=measured_key)
        cluster_total, values, _ = split({"total": 37.0}, tenants(*rows))
        assert sum(values) == pytest.approx(cluster_total)


class TestRungFootprint:
    def test_total_split_by_size(self):
        usage = tenants((101, {"keys": 300}), (102, {"keys": 100}))
        assert_split(rung_footprint("memory_gib", "keys")({"memory_gib": 8.0}, usage), 8.0, [6.0, 2.0], "footprint")

    def test_measured_usage_first_then_rest_by_size(self):
        usage = tenants((101, {"measured_gib": 1.0, "keys": 1}), (102, {"measured_gib": 0.0, "keys": 1}))
        split = rung_footprint("memory_gib", "keys", measured_key="measured_gib")
        assert_split(split({"memory_gib": 5.0}, usage), 5.0, [3.0, 2.0], "footprint")

    def test_no_size_falls_back_to_roster(self):
        usage = tenants((101, {"keys": 0}), (102, {"keys": 0}))
        assert_split(rung_footprint("memory_gib", "keys")({"memory_gib": 8.0}, usage), 8.0, [4.0, 4.0], "roster")

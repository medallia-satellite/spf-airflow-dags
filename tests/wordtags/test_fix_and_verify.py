"""Behaviour of the policy-mapping helpers in ``wordtags.fix_and_verify``. The DAGs take an
optional ``policy_mapping`` param that replaces ``POLICY_MAPPING``; a bad mapping must fail loudly
rather than silently skip tenants, and the policy *name* must stay independent of its retention
(``M6`` can mean 2 months)."""

import pytest

from wordtags.fix_and_verify import (
    POLICY_MAPPING,
    POLICY_MAPPING_PARAM_SCHEMA,
    expected_index_template,
    policy_base_name,
    policy_retention,
    resolve_policy_mapping,
)


class TestResolvePolicyMapping:
    @pytest.mark.parametrize("override", [None, {}])
    def test_falls_back_to_defaults(self, override):
        assert resolve_policy_mapping(override) == POLICY_MAPPING

    def test_defaults_are_keyed_by_base_name(self):
        assert not any(name.endswith("_rollover") for name in POLICY_MAPPING)

    def test_override_replaces_defaults(self):
        assert resolve_policy_mapping({"M6": 2}) == {"M6": 2}

    @pytest.mark.parametrize("override", [{"M6_rollover": 2}, {"M6": 2, "M6_rollover": 2}])
    def test_rollover_names_normalize_to_base(self, override):
        assert resolve_policy_mapping(override) == {"M6": 2}

    def test_normalizes_every_policy(self):
        assert resolve_policy_mapping({"M6": 2, "M18_rollover": 12}) == {"M6": 2, "M18": 12}

    def test_custom_policy_names_are_accepted(self):
        assert resolve_policy_mapping({"tb_custom_rollover": 24}) == {"tb_custom": 24}

    def test_mismatched_counterpart_retention_raises(self):
        with pytest.raises(ValueError, match="same retention"):
            resolve_policy_mapping({"M6": 2, "M6_rollover": 6})

    def test_does_not_mutate_input(self):
        override = {"M6": 2}

        resolve_policy_mapping(override)

        assert override == {"M6": 2}

    @pytest.mark.parametrize("months", [0, -1, "6", 6.0, True, None])
    def test_invalid_retention_raises(self, months):
        with pytest.raises(ValueError, match="positive integer"):
            resolve_policy_mapping({"M6": months, "M6_rollover": months})

    @pytest.mark.parametrize("override", [["M6"], "M6"])
    def test_non_object_raises(self, override):
        with pytest.raises(ValueError, match="must be an object"):
            resolve_policy_mapping(override)


class TestPolicyBaseName:
    @pytest.mark.parametrize(
        "name, base",
        [
            ("M6", "M6"),
            ("M6_rollover", "M6"),
            ("tb_custom_rollover", "tb_custom"),
        ],
    )
    def test_strips_rollover_suffix(self, name, base):
        assert policy_base_name(name) == base


class TestPolicyRetention:
    @pytest.mark.parametrize("name", ["M6", "M6_rollover"])
    def test_base_and_rollover_share_retention(self, name):
        assert policy_retention({"M6": 2}, name) == 2

    @pytest.mark.parametrize("name", ["M18", "M18_rollover", "M6_rollover_rollover", "", None])
    def test_unknown_policy_is_none(self, name):
        assert policy_retention({"M6": 2}, name) is None

    def test_defaults_cover_rollover_names(self):
        assert policy_retention(POLICY_MAPPING, "M36_rollover") == 36


class TestPolicyMappingParamSchema:
    """The Param schema rejects malformed mappings at trigger time. Needs Airflow, which the
    Jenkins test env doesn't install, so these skip there and run locally."""

    @pytest.fixture
    def make_param(self):
        param_module = pytest.importorskip("airflow.models.param")
        return lambda value: param_module.Param(value, **POLICY_MAPPING_PARAM_SCHEMA)

    @pytest.mark.parametrize("value", [None, {}, {"M6": 2}, {"M6": 2, "M6_rollover": 2}])
    def test_accepts_valid(self, make_param, value):
        assert make_param(value).resolve() == value

    @pytest.mark.parametrize("value", [{"M6": 0}, {"M6": -1}, {"M6": "2"}, {"M6": 2.5}, {"M6": True}, ["M6"]])
    def test_rejects_invalid(self, make_param, value):
        exceptions = pytest.importorskip("airflow.exceptions")
        with pytest.raises(exceptions.ParamValidationError):
            make_param(value).resolve()


def test_expected_index_template_uses_policy_name_not_retention():
    template = expected_index_template("acme", "M6")
    lifecycle = template["template"]["settings"]["index"]["lifecycle"]

    assert lifecycle == {"name": "M6_rollover", "rollover_alias": "acme-rollover"}

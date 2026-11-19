"""Behaviour of the policy-mapping helpers in ``wordtags.fix_and_verify``. The DAGs take an
optional ``policy_mapping`` param that replaces ``POLICY_MAPPING``; Airflow validates it against
``POLICY_MAPPING_PARAM_SCHEMA`` when a run is created, so a bad mapping never reaches a task. The
policy *name* must stay independent of its retention (``M6`` can mean 2 months)."""

import jsonschema
import pytest

from wordtags.fix_and_verify import (
    POLICY_MAPPING,
    POLICY_MAPPING_PARAM_SCHEMA,
    expected_index_template,
    policy_base_name,
    policy_retention,
)


class TestPolicyMappingParamSchema:
    @pytest.mark.parametrize("value", [None, {}, {"M6": 2}, {"M6": 2, "tb_custom": 24}])
    def test_accepts_valid(self, value):
        jsonschema.validate(value, POLICY_MAPPING_PARAM_SCHEMA)

    @pytest.mark.parametrize(
        "value",
        [
            {"M6_rollover": 2},  # base names only; M6 covers M6_rollover
            {"M6": 0},
            {"M6": -1},
            {"M6": "2"},
            {"M6": 2.5},
            {"M6": True},
            ["M6"],
        ],
    )
    def test_rejects_invalid(self, value):
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(value, POLICY_MAPPING_PARAM_SCHEMA)

    def test_defaults_match_schema(self):
        jsonschema.validate(POLICY_MAPPING, POLICY_MAPPING_PARAM_SCHEMA)


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


def test_expected_index_template_uses_policy_name_not_retention():
    template = expected_index_template("acme", "M6")
    lifecycle = template["template"]["settings"]["index"]["lifecycle"]

    assert lifecycle == {"name": "M6_rollover", "rollover_alias": "acme-rollover"}

import datetime
import re

from dateutil.relativedelta import relativedelta

BASE_PATTERN = r"(\w+)_topic-builder(-\w+)+(\.\w{2,4}){0,2}(\.\w+)(\.\w{2,4}){1,2}-\1"
BASE_REGEX = re.compile(BASE_PATTERN)
INDEX_PATTERN = (
    rf"^seaas-{BASE_PATTERN}"
    + r"-(?P<month>[0-9]{4}-[0-9]{2}-[0-9]{2})-(?P<tenant_id>[0-9]+)-(?P<suffix>[0-9]+)$"
)
INDEX_REGEX = re.compile(INDEX_PATTERN)
ALIAS_REGEX_MAPPING = {
    "read": re.compile(rf"{BASE_PATTERN}$"),
    "write": re.compile(rf"{BASE_PATTERN}" + r"-[0-9]{4}-[0-9]{2}-[0-9]{2}"),
    "rollover": re.compile(rf"{BASE_PATTERN}-rollover"),
}
# ILM policy base name -> retention months. Each policy `X` has a rollover
# counterpart `X_rollover` with the same retention; look names up with
# `policy_retention`, which covers both.
POLICY_MAPPING = {
    "M6": 6,
    "M18": 18,
    "M36": 36,
}
ROLLOVER_POLICY_SUFFIX = "_rollover"
# Keyword arguments for the `policy_mapping` Airflow Param (JSON schema), so a
# malformed mapping is rejected when the run is triggered. Kept as a plain dict
# to keep this module free of Airflow imports.
POLICY_MAPPING_PARAM_SCHEMA = {
    "type": ["null", "object"],
    "additionalProperties": {"type": "integer", "minimum": 1},
    "description": (
        "Optional ILM policy name -> retention months. Replaces the defaults "
        f"({POLICY_MAPPING}) when set. 'X' also covers 'X_rollover', "
        'e.g. {"M6": 2}.'
    ),
}


def policy_base_name(name: str) -> str:
    """Return the policy name without its rollover suffix (`M6_rollover` -> `M6`)."""
    return name.removesuffix(ROLLOVER_POLICY_SUFFIX)


def policy_retention(mapping: dict[str, int], name: str | None) -> int | None:
    """Return retention months for an ILM policy name (`X` or `X_rollover`), or None if unknown."""
    if not isinstance(name, str):
        return None
    return mapping.get(policy_base_name(name))


def resolve_policy_mapping(override: dict | None) -> dict[str, int]:
    """Return the policy base name -> retention months mapping to use for a run.

    A falsy `override` falls back to `POLICY_MAPPING`; otherwise `override`
    replaces it entirely. Keys may be given as `X` or `X_rollover`; both are
    normalized to `X`, since the two always share a retention. Giving both
    with different retentions is an error.
    """
    if not override:
        return POLICY_MAPPING
    if not isinstance(override, dict):
        raise ValueError(f"policy_mapping must be an object, got {type(override).__name__}")

    resolved: dict[str, int] = {}
    for name, months in override.items():
        if not isinstance(months, int) or isinstance(months, bool) or months <= 0:
            raise ValueError(f"policy_mapping['{name}'] must be a positive integer, got {months!r}")

        base = policy_base_name(name)
        if resolved.get(base, months) != months:
            rollover = f"{base}{ROLLOVER_POLICY_SUFFIX}"
            raise ValueError(
                f"policy_mapping['{base}'] and policy_mapping['{rollover}'] must have the same retention"
            )
        resolved[base] = months

    return resolved


INDEX_TEMPLATE_SETTINGS_INDEX_ANALYSIS = {
    "filter": {
        "compound_capture": {
            "type": "pattern_capture",
            "preserve_original": "false",
            "patterns": ["(!?[^@!@]+)@!@"],
        }
    },
    "analyzer": {
        "topic-builder-analyzer": {
            "filter": ["compound_capture"],
            "type": "custom",
            "tokenizer": "whitespace",
        }
    },
}
INDEX_TEMPLATE_MAPPINGS = {
    "properties": {
        "comments": {
            "type": "nested",
            "properties": {
                "language": {"type": "keyword"},
                "linguisticConnections": {
                    "type": "text",
                    "analyzer": "topic-builder-analyzer",
                    "position_increment_gap": 1000,
                },
                "linguisticConnectionsIndexes": {"type": "short"},
                "name": {"type": "keyword"},
                "persona": {"type": "keyword"},
                "sentenceContent": {
                    "type": "text",
                    "analyzer": "topic-builder-analyzer",
                },
                "sentenceIndex": {"type": "short"},
                "wordEndIndexes": {"type": "integer"},
                "wordStartIndexes": {"type": "integer"},
            },
        },
        "responseDate": {"type": "date"},
        "surveyId": {"type": "long"},
    },
}


def expected_index_template(tenant, policy_base):
    return {
        "index_patterns": [f"seaas-{tenant}-*"],
        "template": {
            "settings": {
                "index": {
                    "lifecycle": {
                        "name": f"{policy_base}{ROLLOVER_POLICY_SUFFIX}",
                        "rollover_alias": f"{tenant}-rollover",
                    },
                    "analysis": INDEX_TEMPLATE_SETTINGS_INDEX_ANALYSIS,
                    "number_of_shards": "1",
                    "number_of_replicas": "1",
                }
            },
            "mappings": INDEX_TEMPLATE_MAPPINGS,
        },
    }


def extract_index_details(index):
    tenant = BASE_REGEX.search(index).group(0)
    m = INDEX_REGEX.fullmatch(index).groupdict()
    month = m["month"]
    origination_date = int(
        datetime.datetime.fromisoformat(month)
        .replace(tzinfo=datetime.timezone.utc)
        .timestamp()
        * 1e3
    )
    return {
        "tenant": tenant,
        "tenant_id": m["tenant_id"],
        "suffix": int(m["suffix"]),
        "month": month,
        "origination_date": origination_date,
        "read_alias": tenant,
        "rollover_alias": f"{tenant}-rollover",
        "write_alias": ALIAS_REGEX_MAPPING["write"].search(index).group(0),
        "should_rollover": (
            datetime.date.fromisoformat(month)
            >= datetime.date.today().replace(day=1) + relativedelta(months=1)
        ),
    }


def index_has_expired(index, retention):
    m = INDEX_REGEX.fullmatch(index).groupdict()
    return is_past_retention_limit(m["month"], retention)


def is_past_retention_limit(iso_date_str: str, retention_months: int) -> bool:
    current_month_start = datetime.datetime.now(datetime.timezone.utc).replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    )
    retention_cutoff = current_month_start - relativedelta(months=retention_months)

    comparison_date = datetime.datetime.fromisoformat(iso_date_str).replace(
        tzinfo=datetime.timezone.utc
    )
    return comparison_date < retention_cutoff


def generate_write_aliases(tenant, retention_months):
    current_month_start = (
        datetime.datetime.today() - relativedelta(months=retention_months)
    ).replace(day=1, hour=0, minute=0, second=0, tzinfo=datetime.timezone.utc)
    return [
        f"{tenant}-{current_month_start + relativedelta(months=i):%Y-%m-%d}"
        for i in range(retention_months + 1)
    ]

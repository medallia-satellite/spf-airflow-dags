import datetime
import functools
import re
from typing import TypedDict, Optional, Any

from dateutil.relativedelta import relativedelta

BASE_PATTERN = r"(\w+)_topic-builder(-\w+)+(\.\w{2,4}){0,2}(\.\w+)(\.\w{2,4}){1,2}-\1"
BASE_REGEX = re.compile(BASE_PATTERN)
INDEX_PATTERN = (
    rf"^seaas-{BASE_PATTERN}"
    + r"-(?P<month>[0-9]{4}-[0-9]{2}-[0-9]{2})-(?P<tenant_id>[0-9]+)-(?P<suffix>[0-9]+)$"
)
INDEX_REGEX = re.compile(INDEX_PATTERN)
ALIAS_REGEX_MAPPING = {
    "read": re.compile(rf"{BASE_PATTERN}"),
    "write": re.compile(rf"{BASE_PATTERN}" + r"-[0-9]{4}-[0-9]{2}-[0-9]{2}"),
    "rollover": re.compile(rf"{BASE_PATTERN}-rollover"),
}
POLICY_MAPPING = {
    "M6": 6,
    "M6_rollover": 6,
    "M18": 18,
    "M18_rollover": 18,
    "M36": 36,
    "M36_rollover": 36,
}


def expected_index_template(tenant, retention_months):
    return {
        "index_patterns": [f"seaas-{tenant}-*"],
        "template": {
            "settings": {
                "index": {
                    "lifecycle": {
                        "name": f"M{retention_months}_rollover",
                        "rollover_alias": f"{tenant}-rollover",
                    },
                    "analysis": {
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
                    },
                    "number_of_shards": "1",
                    "number_of_replicas": "1",
                }
            },
            "mappings": {
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
                }
            },
        },
    }


def extract_index_details(index):
    tenant = BASE_REGEX.search(index).group(0)
    m = INDEX_REGEX.fullmatch(index).groupdict()
    month = m["month"]
    return {
        "tenant": tenant,
        "tenant_id": m["tenant_id"],
        "suffix": int(m["suffix"]),
        "month": month,
        "read_alias": tenant,
        "rollover_alias": f"{tenant}-rollover",
        "write_alias": ALIAS_REGEX_MAPPING["write"].search(index).group(0),
        "should_rollover": (
            datetime.date.fromisoformat(month)
            == datetime.date.today().replace(day=1) + relativedelta(months=1)
        ),
    }


def index_has_expired(index, retention):
    details = extract_index_details(index)
    oldest = datetime.datetime.today().replace(
        day=1, hour=0, minute=0, second=0, tzinfo=datetime.timezone.utc
    ) - relativedelta(months=retention)
    index_datetime = datetime.datetime.fromisoformat(details["month"]).replace(
        tzinfo=datetime.timezone.utc
    )
    return oldest > index_datetime


def generate_past_month_starts(n):
    current_month_start = datetime.datetime.today().replace(
        day=1, hour=0, minute=0, second=0, tzinfo=datetime.timezone.utc
    ) - relativedelta(months=n - 1)
    return [current_month_start + relativedelta(months=i) for i in range(n + 1)]


def chain_on_success(func):
    @functools.wraps(func)
    def wrapper(context):
        if not context["success"]:
            return context
        return func(context)

    return wrapper


def chain_on_error_in_stage(stage):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(context):
            if context["success"] or context["stage"] != stage:
                return context
            return func(context)

        return wrapper

    return decorator


class Context(TypedDict, total=False):
    tenant: str
    tenant_id: int
    success: bool
    error: Optional[Any]
    stage: Optional[str]
    value: Optional[Any]
    retention: Optional[int]
    latest_suffix: Optional[int]
    conn_id: str
    dry_run: bool


def success(
    context: Context,
    stage: str,
    value: Any = None,
) -> Context:
    context.update(
        {
            "success": True,
            "stage": stage,
            "value": value,
            "error": None,
        }
    )
    return context


def failure(
    context: Context,
    stage: str,
    error: Any = None,
) -> Context:
    context.update(
        {
            "success": False,
            "stage": stage,
            "value": None,
            "error": error,
        }
    )
    return context

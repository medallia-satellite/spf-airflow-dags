import datetime
import re
from typing import Iterator, Tuple

from dateutil.relativedelta import relativedelta

BASE_PATTERN = r"(\w+)_topic-builder(-\w+)+(\.\w{2,4}){0,2}(\.\w+)(\.\w{2,4}){1,2}-\1"
BASE_REGEX = re.compile(BASE_PATTERN)
INDEX_PATTERN = rf"^seaas-(?P<tenant>{BASE_PATTERN})" + r"-[0-9]{4}-[0-9]{2}-[0-9]{2}-(?P<tenant_id>[0-9]+)-(?P<suffix>[0-9]+)$"
INDEX_REGEX = re.compile(INDEX_PATTERN)

def generate_monthly_aliases(alias: str, start_date: datetime.date, num_months: int) -> Iterator[Tuple[str, str]]:
    current_date = start_date
    for _ in range(num_months):
        yield f"{current_date:%Y-%m-%d}", f"{alias}-{current_date:%Y-%m-%d}"
        current_date -= relativedelta(months=1)

def generate_aliases(index):
    read_alias = ALIAS_REGEX_MAPPING["read"].search(index).group(0)
    write_alias = ALIAS_REGEX_MAPPING["write"].search(index).group(0)
    rollover_alias = f"{read_alias}-rollover"
    return {
        "read": read_alias,
        "write": write_alias,
        "rollover": rollover_alias,
    }

def is_write_alias(alias: str) -> bool:
    return True if ALIAS_REGEX_MAPPING["write"].match(alias) else False

def tenant_id_from_index(index_name):
    m = INDEX_REGEX.fullmatch(index_name).groupdict()
    return m["tenant_id"]

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
def default_index_settings_and_mappings():
    return {
    "settings": default_index_settings(),
    "mappings": default_index_mappings()
}

def default_index_settings():
    return {
        "analysis": {
            "filter": {
                "compound_capture": {
                    "type": "pattern_capture",
                    "preserve_original": "false",
                    "patterns": [
                        "(!?[^@!@]+)@!@"
                    ]
                }
            },
            "analyzer": {
                "topic-builder-analyzer": {
                    "filter": [
                        "compound_capture"
                    ],
                    "type": "custom",
                    "tokenizer": "whitespace"
                }
            }
        },
        "number_of_shards": "1",
        "number_of_replicas": "1",
    }

def default_index_mappings():
    return {
        "properties": {
            "comments": {
                "type": "nested",
                "properties": {
                    "language": {
                        "type": "keyword"
                    },
                    "linguisticConnections": {
                        "type": "text",
                        "analyzer": "topic-builder-analyzer",
                        "position_increment_gap": 1000
                    },
                    "linguisticConnectionsIndexes": {
                        "type": "short"
                    },
                    "name": {
                        "type": "keyword"
                    },
                    "persona": {
                        "type": "keyword"
                    },
                    "sentenceContent": {
                        "type": "text",
                        "analyzer": "topic-builder-analyzer"
                    },
                    "sentenceIndex": {
                        "type": "short"
                    },
                    "wordEndIndexes": {
                        "type": "integer"
                    },
                    "wordStartIndexes": {
                        "type": "integer"
                    }
                }
            },
            "responseDate": {
                "type": "date"
            },
            "surveyId": {
                "type": "long"
            }
        }
    }

def expected_index_template(tenant, retention_months):
    rollover_alias = f"{tenant}-rollover"
    index_pattern = f"seaas-{tenant}-*"
    index_template = {
        "index_patterns": [index_pattern],
        "template": {
            "settings": {
                "index": default_index_settings()
            },
            "mappings": default_index_mappings()
        }
    }
    index_template["template"]["settings"]["index"]["lifecycle"] = {
        "name": f"M{retention_months}_rollover",
        "rollover_alias": rollover_alias,
    }
    return index_template



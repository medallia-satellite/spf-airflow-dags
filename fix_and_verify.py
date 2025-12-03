import datetime
import re
from typing import Iterator

from dateutil.relativedelta import relativedelta

BASE_PATTERN = r"(\w+)_topic-builder(-\w+)+(\.\w{2,4}){0,2}(\.\w+)(\.\w{2,4}){1,2}-\1"
BASE_REGEX = re.compile(BASE_PATTERN)
INDEX_PATTERN = rf"^seaas-{BASE_PATTERN}" + r"-[0-9]{4}-[0-9]{2}-[0-9]{2}-(?P<tenant_id>[0-9]+)-(?P<suffix>[0-9]+)$"
INDEX_REGEX = re.compile(INDEX_PATTERN)
ALIAS_REGEX_MAPPING = {
    "read": re.compile(rf"^{BASE_PATTERN}$"),
    "write": re.compile(rf"^{BASE_PATTERN}" + r"-[0-9]{4}-[0-9]{2}-[0-9]{2}$"),
    "rollover": re.compile(rf"^{BASE_PATTERN}-rollover$"),
}
POLICY_MAPPING = {
	"M6": 6,
	"M6_rollover": 6,
	"M18": 18,
	"M18_rollover": 18,
	"M36": 36,
	"M36_rollover": 36,
}
INDEX_SETTINGS_AND_MAPPINGS = {
    "settings": {
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
        "number_of_shards": 1,
        "number_of_replicas": 1
    },
    "mappings": {
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
}


def monthly_aliases(alias: str, start_date: datetime.date, num_months: int) -> Iterator[str]:
    current_date = start_date
    for _ in range(num_months):
        yield f"{alias}-{current_date:%Y-%m-%d}"
        current_date -= relativedelta(months=1)

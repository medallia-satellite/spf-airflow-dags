"""seaas tenant index names, shared by the DAGs that parse them.

Kept out of the DAG files because importing a DAG file from another one registers its DAG twice.
"""

import re
from typing import Optional

# Tenant index names, tried in order (<instance> is the instance's hostname):
#   seaas-<in_app_id>_surveys-<instance>-<in_app_id>-<instance_id>-<suffix>
#   seaas-<in_app_id>_topic-builder-<instance>-<in_app_id>-<YYYY-MM-01>-<instance_id>-<suffix>
#     e.g. seaas-pkgdentest_topic-builder-pkgdentest.medallia.com-pkgdentest-2023-10-01-101880-0
_SEAAS_INDEX_BODY = r"-(?P<instance>[\w-]+(?:\.[\w-]+)*)-(?P=in_app_id)"
_SEAAS_INDEX_TAIL = r"-(?P<instance_id>[0-9]+)-(?P<suffix>[0-9]+)$"
SEAAS_INDEX_REGEXES = [
    re.compile(r"^seaas-(?P<in_app_id>\w+)_surveys" + _SEAAS_INDEX_BODY + _SEAAS_INDEX_TAIL),
    re.compile(
        r"^seaas-(?P<in_app_id>\w+)_topic-builder" + _SEAAS_INDEX_BODY
        + r"-(?P<month>[0-9]{4}-[0-9]{2}-[0-9]{2})" + _SEAAS_INDEX_TAIL
    ),
]


def match_seaas_index(index: str) -> Optional[re.Match]:
    """Return the first ``SEAAS_INDEX_REGEXES`` match for ``index``, or None."""
    return next((m for m in (regex.fullmatch(index) for regex in SEAAS_INDEX_REGEXES) if m), None)

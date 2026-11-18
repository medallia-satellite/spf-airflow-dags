"""Decom: the index pattern and summary of ``decom_list_seaas_indices_dag``.

Kept out of the DAG file so it can be tested without Airflow.
"""

import re
from typing import Any, Dict, List

# What a company (its in_app_id) and an instance (its hostname) may look like. Both go into an
# Elasticsearch index pattern, so wildcards and commas must never get through.
COMPANY_PATTERN = r"^\w+$"
INSTANCE_PATTERN = r"^[\w-]+(\.[\w-]+)*$"

# _cat/indices columns to ask for; with bytes=b, store.size is a number of bytes.
CAT_INDICES_COLUMNS = "index,status,docs.count,store.size"


def index_pattern(company: str, instance: str) -> str:
    """Elasticsearch pattern for ``company``'s indices on ``instance``.

    Raises ``ValueError`` unless they match ``COMPANY_PATTERN`` and ``INSTANCE_PATTERN``.
    """
    if not re.fullmatch(COMPANY_PATTERN, company or ""):
        raise ValueError(f"company must be an in_app_id of letters, digits and _, got {company!r}")
    if not re.fullmatch(INSTANCE_PATTERN, instance or ""):
        raise ValueError(f"instance must be a hostname like acme.medallia.eu, got {instance!r}")
    return f"seaas-{company}_*-{instance}-{company}-*"


def parse_indices(cat_indices: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """``{index: {"status", "docs", "bytes"}}`` from a ``_cat/indices`` response, sorted by index.

    The response must have the ``CAT_INDICES_COLUMNS`` with ``bytes=b``. ``docs.count`` is null for
    closed indices, so their ``docs`` is None; a null ``store.size`` counts as 0 bytes.
    """
    return {
        r["index"]: {
            "status": r["status"],
            "docs": None if r["docs.count"] is None else int(r["docs.count"]),
            "bytes": int(r["store.size"] or 0),
        }
        for r in sorted(cat_indices, key=lambda r: r["index"])
    }


def format_bytes(n: int) -> str:
    """Human-readable size like Elasticsearch's: ``512b``, ``1.2kb``, ``3.4mb``, ``1.2gb``, ``2.0tb``."""
    if n < 1024:
        return f"{n}b"
    size = float(n)
    for unit in ("kb", "mb", "gb", "tb"):
        size /= 1024
        if size < 1024 or unit == "tb":
            return f"{size:.1f}{unit}"


def format_cluster(namespace: str, indices: Dict[str, Dict[str, Any]]) -> str:
    """A namespace header, then one line per index.

    ``namespace: <namespace> (<n> indices, <docs> docs, <size>[, <k> closed])``, where docs only
    counts open indices, then ``<index> (<docs> docs, <size>)`` or ``<index> (closed, <size>)``.
    """
    closed = sum(1 for i in indices.values() if i["status"] == "close")
    docs = sum(i["docs"] or 0 for i in indices.values() if i["status"] != "close")
    size = format_bytes(sum(i["bytes"] for i in indices.values()))
    header = f"namespace: {namespace} ({len(indices)} indices, {docs:,} docs, {size}"
    lines = [header + (f", {closed} closed)" if closed else ")")]
    for index, i in indices.items():
        if i["status"] == "close":
            lines.append(f"{index} (closed, {format_bytes(i['bytes'])})")
        else:
            lines.append(f"{index} ({i['docs'] or 0:,} docs, {format_bytes(i['bytes'])})")
    return "\n".join(lines)


def format_summary(per_cluster: List[Dict[str, Any]]) -> str:
    """``format_cluster`` of every cluster with indices, separated by blank lines.

    ``"No indices found"`` if no cluster has any.
    """
    blocks = [format_cluster(c["namespace"], c["indices"]) for c in per_cluster if c["indices"]]
    return "\n\n".join(blocks) or "No indices found"

"""Decom: the index pattern, snapshot check and summary of ``decom_list_seaas_indices_dag``.

Kept out of the DAG file so it can be tested without Airflow.
"""

import re
from datetime import datetime, timedelta
from typing import Any, Dict, List

# What a company (its in_app_id) and an instance (its hostname) may look like. Both go into an
# Elasticsearch index pattern, so wildcards and commas must never get through.
COMPANY_PATTERN = r"^\w+$"
INSTANCE_PATTERN = r"^[\w-]+(\.[\w-]+)*$"

# _cat/indices columns to ask for; with bytes=b, store.size is a number of bytes.
CAT_INDICES_COLUMNS = "index,status,docs.count,store.size"

# Indices with documents must be in a successful snapshot of this repository taken this recently.
SNAPSHOT_REPOSITORY = "es-snapshots"
SNAPSHOT_WINDOW = timedelta(hours=24)
# Only what recent_snapshots needs from GET /_snapshot/<repository>/_all.
SNAPSHOT_FILTER_PATH = "snapshots.snapshot,snapshots.state,snapshots.start_time_in_millis,snapshots.indices"


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


def indices_with_docs(indices: Dict[str, Dict[str, Any]]) -> List[str]:
    """The open indices in ``parse_indices`` output that have documents, i.e. the ones to find a snapshot of."""
    return [index for index, i in indices.items() if i["status"] != "close" and i["docs"]]


def recent_snapshots(snapshots_response: dict, since: datetime) -> Dict[str, str]:
    """``{index: snapshot}``: for each index, the latest ``SUCCESS`` snapshot that started at or after ``since``.

    ``snapshots_response`` is a ``GET /_snapshot/<repository>/_all`` response; ``since`` must be
    timezone-aware.
    """
    since_ms = since.timestamp() * 1000
    recent = sorted(
        (
            s
            for s in snapshots_response.get("snapshots", [])
            if s["state"] == "SUCCESS" and s["start_time_in_millis"] >= since_ms
        ),
        key=lambda s: s["start_time_in_millis"],
    )
    # Later snapshots overwrite earlier ones, so each index ends up with its latest.
    return {index: s["snapshot"] for s in recent for index in s.get("indices", [])}


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

    ``namespace: <namespace> (<n> indices, <docs> docs, <size>[, <k> closed][, <m> without snapshot])``,
    where docs only counts open indices, then ``<index> (<docs> docs, <size>)`` or
    ``<index> (closed, <size>)``. Indices that were checked for a snapshot (they have a ``snapshot``
    key) end in ``, snapshot <name>`` or ``, no snapshot in 24h``.
    """
    closed = sum(1 for i in indices.values() if i["status"] == "close")
    unsnapshotted = sum(1 for i in indices.values() if "snapshot" in i and not i["snapshot"])
    docs = sum(i["docs"] or 0 for i in indices.values() if i["status"] != "close")
    size = format_bytes(sum(i["bytes"] for i in indices.values()))
    header = f"namespace: {namespace} ({len(indices)} indices, {docs:,} docs, {size}"
    header += f", {closed} closed" if closed else ""
    header += f", {unsnapshotted} without snapshot" if unsnapshotted else ""
    lines = [header + ")"]
    for index, i in indices.items():
        if i["status"] == "close":
            details = f"closed, {format_bytes(i['bytes'])}"
        else:
            details = f"{i['docs'] or 0:,} docs, {format_bytes(i['bytes'])}"
        if "snapshot" in i:
            details += f", snapshot {i['snapshot']}" if i["snapshot"] else ", no snapshot in 24h"
        lines.append(f"{index} ({details})")
    return "\n".join(lines)


def format_summary(per_cluster: List[Dict[str, Any]]) -> str:
    """``format_cluster`` of every cluster with indices, separated by blank lines.

    ``"No indices found"`` if no cluster has any.
    """
    blocks = [format_cluster(c["namespace"], c["indices"]) for c in per_cluster if c["indices"]]
    return "\n\n".join(blocks) or "No indices found"

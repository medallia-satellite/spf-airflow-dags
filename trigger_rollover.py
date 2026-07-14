"""
Airflow DAG: Trigger Rollover

This DAG triggers Elasticsearch rollover for tenant indices into the next month,
but only for tenants that pass a set of safety/validity checks.

High-level flow:
1. Discover tenant aliases from `/_cat/aliases`.
2. Resolve each tenant's ILM policy and map it to retention months.
3. Load `*-rollover` index templates and validate they match expected shape.
4. For each valid tenant:
   - Inspect `{tenant}-rollover` alias entries.
   - Ensure latest backing index:
     - matches expected index naming pattern,
     - belongs to the current month,
     - is the write index.
   - Trigger rollover to next month’s index name with:
     - incremented suffix,
     - `index.lifecycle.origination_date` set to next month start (UTC ms),
     - alias updates for write/non-write behavior.

Safety:
- `dry_run` is enabled by default.
- Tenants with invalid policy/template/index state are skipped with logs.

DAG params:
- `conn_id` (str): Airflow connection ID for Elasticsearch (default: `es-testing`).
- `dry_run` (bool): If true, call rollover API with `dry_run=true` (default: `True`).

Notes:
- Uses URL-encoded rollover target index format:
  `<seaas-{tenant}-{now/M+1M{yyyy-MM-dd}}-{tenant_id}-{suffix:06}>`.
- Depends on validation helpers and regex/policy mappings from `fix_and_verify.py`.
"""

import datetime
import json
import logging
from typing import Set, Optional

from airflow.decorators import dag, task
from airflow.models import Param
from dateutil.relativedelta import relativedelta

from fix_and_verify import (
    ALIAS_REGEX_MAPPING,
    POLICY_MAPPING,
    expected_index_template,
    extract_index_details,
    BASE_REGEX,
    INDEX_REGEX,
)
from utils import (
    param_value,
    http_hook_get,
    http_hook_post,
)


def build_rollover_index_name(tenant: str, tenant_id: str, next_suffix: int) -> str:
    """Return URL-encoded rollover target index name for next month.

    Format:
      <seaas-{tenant}-{now/M+1M{yyyy-MM-dd}}-{tenant_id}-{suffix:06}>
    The value is pre-encoded for use in the rollover endpoint path.
    """
    return (
        f"%3Cseaas-{tenant}"
        f"-%7Bnow%2FM+1M%7Byyyy-MM-dd%7D%7D"
        f"-{tenant_id}"
        f"-{next_suffix:06}"
        f"%3E"
    )


def fetch_tenants(conn_id: str) -> Set[str]:
    """Return tenant read aliases discovered from Elasticsearch.

    Queries `/_cat/aliases` and filters alias names by
    `ALIAS_REGEX_MAPPING["read"]`.
    """
    alias_rows = http_hook_get(
        conn_id,
        "/_cat/aliases",
        params={"h": "alias", "s": "alias", "format": "json"},
    )
    return {
        row["alias"]
        for row in alias_rows
        if ALIAS_REGEX_MAPPING["read"].match(row["alias"])
    }


def fetch_tenant_retention(conn_id: str, tenant: str) -> Optional[int]:
    """Return retention months for a tenant from its ILM policy.

    Reads `{tenant}` index lifecycle settings, maps policy name via
    `POLICY_MAPPING`, and returns None when missing/invalid.
    """
    tenant_settings = http_hook_get(
        conn_id,
        f"/{tenant}/_settings/index.lifecycle.name",
        params={"flat_settings": "true"},
    )
    if not tenant_settings:
        logging.error(f"{tenant}: no indices found.")
        return None

    policy_name = tenant_settings[min(tenant_settings)]["settings"].get(
        "index.lifecycle.name"
    )
    if policy_name not in POLICY_MAPPING:
        logging.error(f"{tenant}: invalid retention policy '{policy_name}'.")
        return None

    return POLICY_MAPPING[policy_name]


def retention_by_tenant(conn_id: str) -> dict[str, int]:
    tenants = fetch_tenants(conn_id)
    results = {}
    for tenant in tenants:
        retention = fetch_tenant_retention(conn_id, tenant)
        if retention:
            results[tenant] = retention
    return results


def index_templates_by_tenant(conn_id: str) -> dict[str, dict]:
    """Return validated rollover index templates keyed by tenant.

    Keeps only templates that:
    - match rollover template naming rules,
    - belong to tenants with valid retention policy,
    - match `expected_index_template(...)` (ignoring `composed_of`).
    """
    index_templates = http_hook_get(conn_id, "/_index_template/*-rollover")
    retentions = retention_by_tenant(conn_id)
    results: dict[str, dict] = {}
    for entry in index_templates.get("index_templates", []):
        template_name = entry["name"]
        if not ALIAS_REGEX_MAPPING["rollover"].match(template_name):
            logging.warning(
                f"Skipping invalid rollover template name: '{template_name}'"
            )
            continue

        tenant_match = BASE_REGEX.search(template_name)
        tenant = tenant_match.group(0)
        if tenant not in retentions:
            logging.warning(f"Skipping '{template_name}', invalid tenant '{tenant}'")
            continue

        index_template = entry["index_template"]
        comparable = {k: v for k, v in index_template.items() if k != "composed_of"}
        if comparable != expected_index_template(
            tenant=tenant,
            retention_months=retentions[tenant],
        ):
            logging.error(
                f"{tenant}: index template does not match expected.\n{json.dumps(comparable, indent=2)}"
            )
            continue

        results[tenant] = index_template

    return results


def valid_tenants(conn_id: str) -> Set[str]:
    """Return tenants that have fully validated rollover templates."""
    result = index_templates_by_tenant(conn_id)
    return set(result.keys())


@dag(
    dag_display_name="Trigger Rollover",
    tags=["spf", "elasticsearch"],
    description="Triggers a rollover for the next month for all tenants with valid rollover index templates and retention policies.",
    max_active_runs=1,
    schedule=None,
    catchup=False,
    params={
        "conn_id": Param("es-testing", type="string"),
        "dry_run": Param(True, type="boolean"),
    },
    render_template_as_native_obj=True,
)
def trigger_rollover_dag():

    @task
    def trigger_rollover() -> list[str]:
        """Trigger rollover for eligible tenants and return prior write indices.

        Eligibility checks per tenant:
        - rollover alias exists,
        - latest index matches expected naming pattern,
        - latest index month == current month,
        - latest index is current write index.

        For eligible tenants, POST rollover with optional dry-run and log response.
        Returns list of latest indices for which rollover was attempted.
        """
        dry_run = param_value("dry_run")
        conn_id = param_value("conn_id")

        tenants = valid_tenants(conn_id)
        current_month_start = datetime.date.today().replace(day=1)
        next_month_start = current_month_start + relativedelta(months=1)
        next_month_start_ms = int(
            datetime.datetime.combine(
                next_month_start, datetime.time.min, tzinfo=datetime.timezone.utc
            ).timestamp()
            * 1e3
        )

        rolled_over: list[str] = []

        for tenant in tenants:
            rollover_rows = http_hook_get(
                conn_id,
                f"/_cat/aliases/{tenant}-rollover",
                params={
                    "s": "index",
                    "format": "json",
                    "h": "alias,index,is_write_index",
                },
            )
            if not rollover_rows:
                logging.error(f"{tenant}: no rollover alias entries found.")
                continue

            latest = rollover_rows[-1]
            latest_index = latest["index"]

            if not INDEX_REGEX.match(latest_index):
                logging.error(
                    f"{tenant}: latest index '{latest_index}' does not match expected pattern."
                )
                continue

            details = extract_index_details(latest_index)
            index_month = datetime.date.fromisoformat(details["month"])

            if index_month != current_month_start:
                logging.info(
                    f"{tenant}: latest index is {latest_index}, not current month. Skipping."
                )
                continue

            if latest["is_write_index"] != "true":
                logging.error(
                    f"{tenant}: latest index '{latest_index}' is not the write index."
                )
                continue

            rollover_target = build_rollover_index_name(
                tenant=tenant,
                tenant_id=details["tenant_id"],
                next_suffix=details["suffix"] + 1,
            )
            payload = {
                "settings": {
                    "index.lifecycle.origination_date": next_month_start_ms,
                },
                "aliases": {
                    tenant: {"is_write_index": False},
                    f"{tenant}-{next_month_start}": {"is_write_index": True},
                },
            }

            logging.info(
                f"{tenant}: rolling over to {rollover_target} (dry-run={dry_run}):\n"
                f"{json.dumps(payload, indent=2)}"
            )

            response = http_hook_post(
                conn_id,
                f"{tenant}-rollover/_rollover/{rollover_target}",
                json.dumps(payload),
                params={"dry_run": str(dry_run).lower()},
            )

            logging.info(f"Response:\n{json.dumps(response, indent=2)}")

            rolled_over.append(latest_index)

        logging.info(f"Rolled over {len(rolled_over)} indices.")
        return rolled_over

    trigger_rollover()


trigger_rollover_dag()

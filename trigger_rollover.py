import datetime
import json
import logging

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
    """Build the URL-encoded index name for the rollover target."""
    return (
        f"%3Cseaas-{tenant}"
        f"-%7Bnow%2FM+1M%7Byyyy-MM-dd%7D%7D"
        f"-{tenant_id}"
        f"-{next_suffix:06}"
        f"%3E"
    )


def fetch_retention_by_tenant(conn_id: str) -> dict[str, int]:
    """Fetch all read aliases and resolve their ILM retention policy."""
    alias_rows = http_hook_get(
        conn_id,
        "/_cat/aliases",
        params={"h": "alias", "s": "alias", "format": "json"},
    )
    tenants = {
        row["alias"]
        for row in alias_rows
        if ALIAS_REGEX_MAPPING["read"].match(row["alias"])
    }

    retention_by_tenant: dict[str, int] = {}
    for tenant in tenants:
        tenant_settings = http_hook_get(
            conn_id,
            f"/{tenant}/_settings/index.lifecycle.name",
            params={"flat_settings": "true"},
        )
        if not tenant_settings:
            logging.error(f"{tenant}: no indices found.")
            continue

        policy_name = tenant_settings[min(tenant_settings)]["settings"].get(
            "index.lifecycle.name"
        )
        if policy_name not in POLICY_MAPPING:
            logging.error(f"{tenant}: invalid retention policy '{policy_name}'.")
            continue

        retention_by_tenant[tenant] = POLICY_MAPPING[policy_name]

    return retention_by_tenant


def fetch_valid_templates_by_tenant(
    conn_id: str, retention_by_tenant: dict[str, int]
) -> dict[str, dict]:
    """Fetch rollover index templates and validate them against expected structure."""
    template_rows = http_hook_get(conn_id, "/_index_template/*-rollover")

    valid_templates: dict[str, dict] = {}
    for row in template_rows.get("index_templates", []):
        template_name = row["name"]
        if not ALIAS_REGEX_MAPPING["rollover"].match(template_name):
            logging.warning(f"Skipping invalid rollover template name: '{template_name}'")
            continue

        tenant_match = BASE_REGEX.search(template_name)
        if not tenant_match:
            logging.warning(f"Skipping template with unrecognized tenant pattern: '{template_name}'")
            continue

        tenant = tenant_match.group(0)
        if tenant not in retention_by_tenant:
            logging.warning(f"{tenant}: skipping '{template_name}', no retention policy found.")
            continue

        index_template = row["index_template"]
        comparable = {k: v for k, v in index_template.items() if k != "composed_of"}
        if comparable != expected_index_template(
            tenant=tenant,
            retention_months=retention_by_tenant[tenant],
        ):
            logging.error(f"{tenant}: index template does not match expected.\n{json.dumps(comparable, indent=2)}")
            continue

        valid_templates[tenant] = index_template

    return valid_templates


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
        dry_run = param_value("dry_run")
        conn_id = param_value("conn_id")

        retention_by_tenant = fetch_retention_by_tenant(conn_id)
        valid_templates_by_tenant = fetch_valid_templates_by_tenant(conn_id, retention_by_tenant)

        current_month_start = datetime.date.today().replace(day=1)
        next_month_start = current_month_start + relativedelta(months=1)
        next_month_start_ms = int(
            datetime.datetime.combine(
                next_month_start, datetime.time.min, tzinfo=datetime.timezone.utc
            ).timestamp()
            * 1e3
        )

        rolled_over: list[str] = []

        for tenant in valid_templates_by_tenant:
            rollover_rows = http_hook_get(
                conn_id,
                f"/_cat/aliases/{tenant}-rollover",
                params={"s": "index", "format": "json", "h": "alias,index,is_write_index"},
            )
            if not rollover_rows:
                logging.error(f"{tenant}: no rollover alias entries found.")
                continue

            latest = rollover_rows[-1]
            latest_index = latest["index"]

            if not INDEX_REGEX.match(latest_index):
                logging.error(f"{tenant}: latest index '{latest_index}' does not match expected pattern.")
                continue

            details = extract_index_details(latest_index)
            index_month = datetime.date.fromisoformat(details["month"])

            if index_month != current_month_start:
                logging.info(f"{tenant}: latest index is {latest_index}, not current month—skipping.")
                continue

            if latest["is_write_index"] != "true":
                logging.error(f"{tenant}: latest index '{latest_index}' is not the write index.")
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
                params={"dry_run":  str(dry_run).lower()},
            )

            logging.info(f"Response:\n{json.dumps(response, indent=2)}")

            rolled_over.append(latest_index)

        logging.info(f"Rolled over {len(rolled_over)} indices.")
        return rolled_over

    trigger_rollover()


trigger_rollover_dag()
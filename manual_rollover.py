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
)
from utils import (
    http_hook_get,
    param_value,
    http_hook_post,
)


@dag(
    dag_display_name="An awesome dag",
    tags=["spf", "elasticsearch"],
    description="A great description",
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
    def trigger_rollover():
        # fetch
        dry_run = param_value("dry_run")
        conn_id = param_value("conn_id")

        fetched = http_hook_get(
            conn_id,
            "/_cat/aliases",
            params={"h": "alias", "s": "alias", "format": "json"},
        )
        tenants = set(
            r["alias"] for r in fetched if ALIAS_REGEX_MAPPING["read"].match(r["alias"])
        )

        # verify retention
        retention = {}
        for tenant in tenants:
            results = http_hook_get(
                conn_id,
                f"/{tenant}/_settings/index.lifecycle.name",
                params={"flat_settings": "true"},
            )
            if not results:
                logging.error(f"{tenant} - No indices found.")
                continue

            policy_name = results[min(results)]["settings"]["index.lifecycle.name"]
            if policy_name not in POLICY_MAPPING:
                logging.error(f"{tenant} - Invalid retention policy: {policy_name}")
                continue

            retention[tenant] = POLICY_MAPPING[policy_name]

        # verify index template
        index_templates = {}
        results = http_hook_get(conn_id, "/_index_template/*-rollover")
        for r in results["index_templates"]:
            if not ALIAS_REGEX_MAPPING["rollover"].match(r["name"]):
                logging.warning(f"Invalid rollover alias: {r['name']}")
                continue

            tenant = BASE_REGEX.search(r["name"]).group(0)
            if tenant not in retention:
                logging.warning(
                    f"{tenant} - Skipping {r['name']} as it doesn't have a retention policy."
                )
                continue

            index_template = r["index_template"]
            comparable = {k: v for k, v in index_template.items() if k != "composed_of"}
            if comparable != expected_index_template(
                tenant=tenant, retention_months=retention[tenant]
            ):
                dict1 = comparable
                dict2 = expected_index_template(
                tenant=tenant, retention_months=retention[tenant]
            )
                diff = {k: dict2[k] for k in dict2 if dict2.get(k) != dict1.get(k)}
                logging.error(f"{tenant} template does not match expected\n{index_template}\n{diff}")
                continue
            index_templates[tenant] = index_template

        tenants = index_templates.keys()

        for tenant in tenants:

            # verify latest rollover alias is current month
            rollover = http_hook_get(
                conn_id,
                f"/_cat/aliases/{tenant}-rollover",
                params={
                    "s": "index",
                    "format": "json",
                    "h": "alias,index,is_write_index",
                },
            )[-1]
            last_index = rollover["index"]
            logging.info(f"{tenant} - {last_index}")
            details = extract_index_details(last_index)

            if datetime.date.fromisoformat(
                details["month"]
            ) != datetime.date.today().replace(day=1):
                logging.info(f"{tenant} - Skipping {last_index}")
                continue

            if not rollover["is_write_index"] == "true":
                logging.error(f"{tenant} - {last_index} is not write index")
                continue

            target_date = datetime.datetime.today().replace(
                        day=1,
                        hour=0,
                        minute=0,
                        second=0,
                        microsecond=0,
                        tzinfo=datetime.timezone.utc,
                    ) + relativedelta(months=1)
            origination_date = int(target_date.timestamp() * 1e3)

            payload = {
                "settings": {
                    "index.lifecycle.origination_date": origination_date,
                },
                "aliases": {
                    tenant: {"is_write_index": False},
                    f"{tenant}-{str(target_date.date())}": {"is_write_index": True},
                },
            }
            provided_name = f"%3Cseaas-{tenant}-%7Bnow%2FM+1M%7Byyyy-MM-dd%7D%7D-{details['tenant_id']}-{details['suffix']+1:06}%3E"
            logging.info(
                f"Rolling over: {tenant} (dry-run={dry_run})\n{provided_name}\n{json.dumps(payload, indent=2)}"
            )
            if not dry_run:
                response = http_hook_post(
                    conn_id,
                    f"{tenant}-rollover/_rollover/{provided_name}",
                    json.dumps(payload),
                )
                print(f"Response:\n{json.dumps(response, indent=2)}")

    trigger_rollover()


trigger_rollover_dag()

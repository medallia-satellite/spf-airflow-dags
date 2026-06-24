import datetime
import json
import logging
from collections import defaultdict
from typing import List

from airflow.decorators import dag, task
from airflow.models import Param

from fix_and_verify import (
    POLICY_MAPPING,
    extract_index_details,
    index_has_expired,
    ALIAS_REGEX_MAPPING,
    INDEX_REGEX,
)
from utils import (
    http_hook_get,
    http_hook_put,
    Context,
    success,
    failure,
    param_value,
    chain_on_success,
    http_hook_post,
)


def update_index_settings(conn_id: str, index, payload):
    return http_hook_put(conn_id, f"{index}/_settings", json.dumps(payload))


@dag(
    dag_display_name="Elasticsearch ILM Metadata Fix",
    tags=["spf", "elasticsearch"],
    description="Fixes ILM origination-date metadata and finalizes expired indices in Elasticsearch.",
    max_active_runs=1,
    schedule=None,
    catchup=False,
    params={
        "conn_id": Param("es-testing", type="string"),
        "dry_run": Param(True, type="boolean"),
    },
    render_template_as_native_obj=True,
)
def es_index_lifecycle_metadata_fix():
    @task
    def reconcile_origination_dates():
        dry_run = param_value("dry_run")
        conn_id = param_value("conn_id")

        fetched = http_hook_get(
            conn_id,
            "/_all/_settings/index.lifecycle.origination_date,index.creation_date",
            params={"flat_settings": "true"},
        )
        mismatched_indices_by_month = defaultdict(list)
        for index, r in fetched.items():
            if not INDEX_REGEX.match(index):
                continue
            settings = r["settings"]
            origination_date_ms = int(
                settings.get(
                    "index.lifecycle.origination_date", settings["index.creation_date"]
                )
            )

            origination_date = datetime.datetime.fromtimestamp(
                origination_date_ms * 1e-3, tz=datetime.timezone.utc
            ).date()

            index_date_str = extract_index_details(index)["month"]
            index_date = datetime.date.fromisoformat(index_date_str)
            if origination_date != index_date:
                logging.info(
                    f"{index}: should be {index_date} instead of {origination_date} ({settings})."
                )
                mismatched_indices_by_month[index_date_str].append(index)

        add_alias_actions = []
        for year_month, indices in mismatched_indices_by_month.items():
            for index in indices:
                add_alias_actions.append(
                    {
                        "add": {
                            "index": index,
                            "alias": f"temp-{year_month}",
                            "is_write_index": False,
                        }
                    }
                )
        logging.info(
            f"Adding temporal aliases to update (dry-run={dry_run}): {add_alias_actions}"
        )

        if add_alias_actions and not dry_run:
            response = http_hook_post(
                conn_id, "/_aliases/", json.dumps({"actions": add_alias_actions})
            )
            logging.info(f"Response:\n{json.dumps(response, indent=2)}")

        for year_month in mismatched_indices_by_month.keys():
            origination_date = int(
                datetime.datetime.fromisoformat(year_month)
                .replace(tzinfo=datetime.timezone.utc)
                .timestamp()
                * 1e3
            )
            logging.info(
                f"Updating origination_date in alias 'temp-{year_month}' (dry-run={dry_run}): {mismatched_indices_by_month.get(year_month)} to {origination_date}"
            )
            if not dry_run:
                response = update_index_settings(
                    param_value("conn_id"),
                    f"temp-{year_month}",
                    {"index.lifecycle.origination_date": origination_date},
                )
                logging.info(f"Response:\n{json.dumps(response, indent=2)}")

        remove_alias_actions = [
            {"remove": {"index": "*", "alias": f"temp-{year_month}"}}
            for year_month in mismatched_indices_by_month.keys()
        ]

        logging.info(
            f"Removing temporal aliases (dry-run={dry_run}): {remove_alias_actions}"
        )

        if remove_alias_actions and not dry_run:
            response = http_hook_post(
                conn_id, "/_aliases/", json.dumps({"actions": remove_alias_actions})
            )
            logging.info(f"Response:\n{json.dumps(response, indent=2)}")

        return mismatched_indices_by_month

    @task
    def fetch_tenants() -> List[Context]:
        fetched = http_hook_get(
            param_value("conn_id"),
            "/_cat/aliases",
            params={"h": "alias", "s": "alias", "format": "json"},
        )

        return [
            Context(success=True, tenant=alias)
            for alias in set(
                r["alias"]
                for r in fetched
                if ALIAS_REGEX_MAPPING["read"].match(r["alias"])
            )
        ]

    @task
    def fetch_retention(context: Context) -> Context:
        results = http_hook_get(
            param_value("conn_id"),
            "/_settings/index.lifecycle.name",
            params={"flat_settings": "true"},
        )
        logging.info(f"Fetched {len(results)} records")
        oldest_index = min(results)
        policy_name = results[oldest_index]["settings"]["index.lifecycle.name"]
        if policy_name not in POLICY_MAPPING:
            return failure(
                context=context,
                stage="fetch_retention",
                error=f"Invalid retention policy: '{policy_name}'",
            )

        return Context(
            success=True,
            tenant=context["tenant"],
            retention=POLICY_MAPPING.get(policy_name),
        )

    @task
    @chain_on_success
    def expired_indices(context: Context) -> Context:
        stage = "expired_indices"
        conn_id = param_value("conn_id")

        results = http_hook_get(
            conn_id,
            f"/_cat/indices/{context['tenant']}",
            params={
                "s": "index",
                "h": "index",
                "format": "json",
            },
        )
        indices = [r["index"] for r in results if INDEX_REGEX.match(r["index"])]
        if not indices:
            return failure(
                context=context,
                stage=stage,
                error=f"No indices found for '{context['tenant']}'",
            )

        expired = []
        for index in indices:
            if index_has_expired(index, context["retention"]):
                logging.info(
                    f"Marking indexing as completed (dry-run={param_value('dry_run')}): {index}"
                )
                expired.append(index)

                if param_value("dry_run"):
                    continue

                details = extract_index_details(index)
                origination_date = details["origination_date"]
                response = update_index_settings(
                    param_value("conn_id"),
                    index,
                    {
                        "index.lifecycle.indexing_complete": True,
                        "index.lifecycle.origination_date": origination_date,
                    },
                )
                logging.info(f"Response:\n{json.dumps(response, indent=2)}")
        return success(context=context, stage=stage, value=expired)

    @task
    def report(contexts: List[Context]):
        for i, c in enumerate(contexts):
            if c["value"]:
                logging.info(f"{i}: {c['tenant']}\n{c['value']}")
            elif not c["success"]:
                logging.error(f"{i}: {c['tenant']} - {c['error']}")

    t1 = reconcile_origination_dates()
    t2 = report(
        expired_indices.expand(context=fetch_retention.expand(context=fetch_tenants()))
    )


es_index_lifecycle_metadata_fix()

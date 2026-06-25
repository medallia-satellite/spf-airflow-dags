import datetime
import json
import logging
from collections import defaultdict

from airflow.decorators import dag, task
from airflow.models import Param

from fix_and_verify import (
    POLICY_MAPPING,
    extract_index_details,
    index_has_expired,
    ALIAS_REGEX_MAPPING,
    INDEX_REGEX,
    BASE_REGEX,
)
from utils import (
    http_hook_get,
    http_hook_put,
    param_value,
    http_hook_post,
)


def apply_index_settings(index, payload, conn_id: str, dry_run: bool):
    logging.info(
        f"Updating '{index}' settings (dry-run={dry_run}): \n{json.dumps(payload, indent=2)}"
    )
    if not dry_run:
        response = http_hook_put(conn_id, f"{index}/_settings", json.dumps(payload))
        logging.info(f"Response:\n{json.dumps(response, indent=2)}")


def apply_alias_actions(actions: list[dict], conn_id: str, dry_run: bool):
    logging.info(
        f"Applying alias updates (dry-run={dry_run}): \n{json.dumps(actions, indent=2)}"
    )

    if actions and not dry_run:
        response = http_hook_post(
            conn_id, "/_aliases/", json.dumps({"actions": actions})
        )
        logging.info(f"Response:\n{json.dumps(response, indent=2)}")


@dag(
    dag_display_name="Index Lifecycle Metadata Fix",
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
        indices_needing_origination_fix_by_month = defaultdict(list)
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
                indices_needing_origination_fix_by_month[index_date_str].append(index)

        add_tmp_alias_actions = []
        for index_date_str, indices in indices_needing_origination_fix_by_month.items():
            for index in indices:
                add_tmp_alias_actions.append(
                    {
                        "add": {
                            "index": index,
                            "alias": f"temp-reconcile_origination_dates-{index_date_str}",
                            "is_write_index": False,
                        }
                    }
                )
        apply_alias_actions(add_tmp_alias_actions, conn_id, dry_run)

        for index_date_str in indices_needing_origination_fix_by_month.keys():
            origination_date = int(
                datetime.datetime.fromisoformat(index_date_str)
                .replace(tzinfo=datetime.timezone.utc)
                .timestamp()
                * 1e3
            )

            apply_index_settings(
                f"temp-reconcile_origination_dates-{index_date_str}",
                {"index.lifecycle.origination_date": origination_date},
                conn_id,
                dry_run,
            )

        remove_tmp_alias_actions = [
            {
                "remove": {
                    "index": "*",
                    "alias": f"temp-reconcile_origination_dates-{year_month}",
                }
            }
            for year_month in indices_needing_origination_fix_by_month.keys()
        ]

        apply_alias_actions(remove_tmp_alias_actions, conn_id, dry_run)

        return indices_needing_origination_fix_by_month

    @task
    def mark_indexing_complete():
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

        results = http_hook_get(
            conn_id,
            f"/_cat/indices",
            params={
                "s": "index",
                "h": "index",
                "format": "json",
            },
        )

        indices = [
            r["index"]
            for r in results
            if INDEX_REGEX.match(r["index"])
            and BASE_REGEX.search(r["index"]).group(0) in retention
        ]

        results = http_hook_get(
            conn_id,
            f"/_all/_settings/index.lifecycle.indexing_complete",
            params={"flat_settings": "true"},
        )

        already_marked = [
            index
            for index, details in results.items()
            if details["settings"]["index.lifecycle.indexing_complete"] == "true"
        ]

        expired = [
            index
            for index in indices
            if index_has_expired(index, retention[BASE_REGEX.search(index).group(0)])
        ]

        add_alias_actions = [
            {
                "add": {
                    "index": index,
                    "alias": f"temp-expire_indices",
                    "is_write_index": False,
                }
            }
            for index in expired
            if index not in already_marked
        ]

        apply_alias_actions(add_alias_actions, conn_id, dry_run)

        if add_alias_actions:
            apply_index_settings(
                f"temp-expire_indices",
                {"index.lifecycle.indexing_complete": True},
                conn_id,
                dry_run,
            )

            remove_alias_actions = [
                {"remove": {"index": "*", "alias": f"temp-expire_indices"}}
            ]
            apply_alias_actions(remove_alias_actions, conn_id, dry_run)

        return expired

    reconcile_origination_dates() >> mark_indexing_complete()


es_index_lifecycle_metadata_fix()

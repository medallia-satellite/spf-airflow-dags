import json
import logging
from typing import List

from airflow.decorators import dag, task
from airflow.models import Param

from fix_and_verify import (
    BASE_REGEX,
    POLICY_MAPPING,
    extract_index_details,
    index_has_expired, ALIAS_REGEX_MAPPING, INDEX_REGEX,
)
from utils import (
    http_hook_get,
    http_hook_put,
    Context,
    success,
    failure,
    param_value,
)


def update_index_settings(conn_id: str, index, payload):
    return http_hook_put(conn_id, f"{index}/_settings", json.dumps(payload))


@dag(
    dag_display_name="Awesome display name",
    tags=["spf", "elasticsearch"],
    description="This is an awesome description for this dat",
    max_active_runs=1,
    schedule=None,
    catchup=False,
    params={
        "conn_id": Param("es-testing", type="string"),
        "dry_run": Param(True, type="boolean"),
    },
    render_template_as_native_obj=True,
)
def testing_dag():
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
                r["alias"] for r in fetched if ALIAS_REGEX_MAPPING["read"].match(r["alias"])
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
    def report(contexts: List[Context]) -> None:
        for i, c in enumerate(contexts):
            if c["value"]:
                logging.info(f"{i}: {c['tenant']}\n{c['value']}")

    report(
        expired_indices.expand(context=fetch_retention.expand(context=fetch_tenants()))
    )


testing_dag()

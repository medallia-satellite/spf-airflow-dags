import json
import os
from typing import List

from airflow.decorators import dag, task
from airflow.models import Param
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from fix_and_verify import (
    INDEX_REGEX,
    extract_index_details,
    generate_write_aliases,
)
from utils import (
    http_hook_put,
    http_hook_get,
    chain_on_error_in_stage,
    Context,
    success,
    failure,
)


def fetch_indices(prefix: str, conn_id: str) -> List[str]:
    results = http_hook_get(conn_id, f"/_cat/indices/{prefix}?h=index&format=json")
    return [r["index"] for r in results if INDEX_REGEX.match(r["index"])]


def create_index(index, payload, conn_id):
    return http_hook_put(conn_id, index, json.dumps(payload))


@dag(
    dag_display_name="Fix Monthly Indices",
    tags=["spf", "elasticsearch"],
    description="This DAG replaces fix and verify job.",
    catchup=False,
    params={
        "conn_id": Param("es-testing", type="string"),
        "dry_run": Param(True, type="boolean"),
        "tenant": Param(
            "spftesting_topic-builder-spf.medallia.com-spftesting", type="string"
        ),
        "tenant_id": Param(12345, type="integer"),
        "retention": Param(6, type="integer"),
    },
    render_template_as_native_obj=True,
)
def fix_monthly_indices_dag():
    @task
    def fetch(context: Context) -> Context:
        tenant: str = context["tenant"]
        fetched = fetch_indices(prefix=f"seaas-{tenant}-*", conn_id=context["conn_id"])
        latest_suffix = 0
        for index in fetched:
            m = INDEX_REGEX.fullmatch(index).groupdict()
            assert (
                int(m["tenant_id"]) == context["tenant_id"]
            ), f"{index}: {m['tenant_id']} != {context['tenant_id']}"
            latest_suffix = max(latest_suffix, int(m["suffix"]))

        context.update({"latest_suffix": latest_suffix})

        return success(context, tenant, value=fetched)

    @task
    def verify(context: Context) -> Context:
        missing = []
        for write_alias in generate_write_aliases(
            context["tenant"], context["retention"]
        ):
            if not any(
                index.startswith(f'seaas-{write_alias}-{context["tenant_id"]}')
                for index in context["value"]
            ):
                missing.append(write_alias)

        if missing:
            return failure(context=context, error=missing, stage="verify")
        return success(context=context, stage="verify")

    @task
    @chain_on_error_in_stage(stage="verify")
    def fix(context: Context) -> None:
        suffix = context["latest_suffix"]

        for write_alias in context["error"]:
            suffix += 1
            index = f'seaas-{write_alias}-{context["tenant_id"]}-{suffix:06}'
            details = extract_index_details(index)
            payload = {
                "settings": {
                    "index.lifecycle.origination_date": details["origination_date"]
                },
                "aliases": {
                    details["read_alias"]: {"is_write_index": False},
                    details["write_alias"]: {"is_write_index": True},
                    details["rollover_alias"]: {"is_write_index": False},
                },
            }
            print(
                f"Creating index: {index} (dry-run={context['dry_run']})\n{json.dumps(payload, indent=2)}"
            )
            if not context["dry_run"]:
                response = create_index(index, payload, context["conn_id"])
                print(f"Response:\n{json.dumps(response, indent=2)}")

    initial_context = Context(
        tenant="{{ params.tenant }}",
        tenant_id="{{ params.tenant_id }}",
        retention="{{ params.retention }}",
        conn_id="{{ params.conn_id }}",
        dry_run="{{ params.dry_run }}",
    )

    fix(verify(fetch(initial_context)))


fix_monthly_indices_dag()

import json
from typing import List

from airflow.decorators import dag, task
from airflow.models import Param

from repo.fix_and_verify import (
    INDEX_REGEX,
    extract_index_details,
    generate_past_month_starts, Context, success, failure,
)
from repo.utils import http_hook_put, http_hook_get


def fetch_indices(prefix: str, conn_id: str) -> List[str]:
    results = http_hook_get(conn_id, f"/_cat/indices/{prefix}*?h=index&format=json")
    return [r["index"] for r in results if INDEX_REGEX.match(r["index"])]

def create_index(context, index, payload):
    if context["dry_run"]:
        print(f"Dry run: {index} - {payload}")
        return
    response = http_hook_put(context["conn_id"], index, json.dumps(payload))
    print(f"{index}: {response}")


@dag(
    dag_display_name="Fix Monthly Indices",
    tags=["spf", "elasticsearch"],
    description="This DAG replaces fix and verify job.",
    max_active_runs=1,
    catchup=False,
    params={
        "db_conn": Param("es-testing", type="string"),
        "dry_run": Param(True, type="boolean"),
        "tenant": Param(type="string"),
        "tenant_id": Param(type="integer"),

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
            assert m["tenant"] == context["tenant"]
            assert m["tenant_id"] == context["tenant_id"]
            latest_suffix = max(latest_suffix, m["suffix"])

        context.update({"latest_suffix": latest_suffix})

        return success(context, tenant, value=fetched)

    @task
    def verify(context: Context) -> Context:
        missing = []
        for month_start in generate_past_month_starts(context["retention"]):
            if not any(
                    index.startswith(
                        f'seaas-{context["tenant"]}-{month_start:%Y-%m-%d}-{context["tenant_id"]}'
                    )
                    for index in context["value"]
            ):
                missing.append(month_start)

        if missing:
            return failure(context=context, error=missing, stage="verify")
        return success(context=context, stage="verify")

    @task
    def fix(context: Context) -> None:
        suffix = context["latest_suffix"]

        for month_start in context["value"]:
            suffix += 1
            origination_date = int(month_start.timestamp() * 1e3)
            index = f'seaas-{context["tenant"]}-{month_start:%Y-%m-%d}-{context["tenant_id"]}-{suffix:06}'
            details = extract_index_details(index)
            payload = {
                "settings": {"index.lifecycle.origination_date": origination_date},
                "aliases": {
                    details["read_alias"]: {"is_write_index": False},
                    details["write_alias"]: {"is_write_index": True},
                    details["rollover_alias"]: {"is_write_index": False},
                },
            }
            create_index(context, index, payload)

    initial_context = Context(
        tenant="{{ params.tenant }}",
        tenant_id="{{ params.tenant_id }}",
        conn_id="{{ params.db_conn }}",
        dry_run="{{ params.dry_run }}",
    )

    fix(verify(fetch(initial_context)))


fix_monthly_indices_dag()

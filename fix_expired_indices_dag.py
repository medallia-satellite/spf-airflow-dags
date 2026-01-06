import json
from typing import List

from airflow.decorators import dag, task
from airflow.models import Param

from repo.fix_and_verify import (
    INDEX_REGEX,
    Context,
    success,
    failure,
    chain_on_error_in_stage,
    index_has_expired,
)
from repo.utils import http_hook_put, http_hook_get


def fetch_indices(prefix: str, conn_id: str) -> List[str]:
    results = http_hook_get(conn_id, f"/_cat/indices/{prefix}*?h=index&format=json")
    return [r["index"] for r in results if INDEX_REGEX.match(r["index"])]


def create_index(index, payload, conn_id):
    return http_hook_put(conn_id, index, json.dumps(payload))


def update_index_settings(conn_id: str, index, payload):
    return http_hook_put(conn_id, f"{index}/_settings", json.dumps(payload))


@dag(
    dag_display_name="Fix Expired Indices",
    tags=["spf", "elasticsearch"],
    description="This DAG replaces fix and verify job.",
    catchup=False,
    params={
        "db_conn": Param("es-testing", type="string"),
        "dry_run": Param(True, type="boolean"),
        "tenant": Param(
            "spftesting_topic-builder-spf.medallia.com-spftesting", type="string"
        ),
        "tenant_id": Param(12345, type="integer"),
        "retention": Param(6, type="integer"),
    },
    render_template_as_native_obj=True,
)
def fix_expired_indices_dag():
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
        expired = []
        for index in context["value"]:
            if index_has_expired(index, context["retention"]):
                expired.append(index)
        if expired:
            return failure(context=context, stage="verify", error=expired)
        return success(context=context, stage="verify")

    @task
    @chain_on_error_in_stage(stage="verify")
    def fix(context: Context) -> Context:
        # Marking indexing as completed unblocks ILMs retention lifecycle when rollovers are performed manually.
        for index in context["error"]:
            print(
                f"Marking indexing as completed (dry-run={context['dry_run']}): {index}"
            )
            if not context["dry_run"]:
                response = update_index_settings(
                    context["conn_id"],
                    index,
                    {"index.lifecycle.indexing_complete": True},
                )
                print(f"Response:\n{json.dumps(response, indent=2)}")

    initial_context = Context(
        tenant="{{ params.tenant }}",
        tenant_id="{{ params.tenant_id }}",
        retention="{{ params.retention }}",
        conn_id="{{ params.db_conn }}",
        dry_run="{{ params.dry_run }}",
    )

    fix(verify(fetch(initial_context)))


fix_expired_indices_dag()

import json
from typing import List

from airflow.decorators import dag, task, task_group
from airflow.models import Param

from fix_and_verify import (
    INDEX_REGEX,
    Context,
    success,
    failure,
    chain_on_error_in_stage,
    index_has_expired,
)
from utils import http_hook_post, http_hook_get


def fetch_indices(prefix: str, conn_id: str) -> List[str]:
    results = http_hook_get(conn_id, f"/_cat/indices/{prefix}?h=index&format=json")
    return [r["index"] for r in results if INDEX_REGEX.match(r["index"])]


def update_aliases(context, actions):
    return http_hook_post(
        context["conn_id"],
        "/_aliases/",
        json.dumps({"actions": actions}),
    )

@dag(
    dag_display_name="Fix Aliases",
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
def fix_aliases_dag():

    @task_group
    def read_alias(upstream: Context) -> Context:
        stage = "read_alias"
        @task
        def fetch(context: Context) -> Context:
            tenant: str = context["tenant"]
            discovered = fetch_indices(prefix=f"seaas-{tenant}-*", conn_id=context["conn_id"])
            aliased = fetch_indices(prefix=f"{tenant}", conn_id=context["conn_id"])
            return success(context, tenant, value={
                "discovered": discovered,
                "aliased": aliased,
            })

        @task
        def verify(context: Context) -> Context:
            discovered = set(context["value"]["discovered"])
            aliased = set(context["value"]["aliased"])
            if len(discovered) != len(aliased):
                return failure(
                    context=context, stage=stage, error=list(discovered - aliased)
                )
            return success(context=context, stage=stage)

        @task
        @chain_on_error_in_stage(stage=stage)
        def fix(context: Context) -> Context:
            alias = context["tenant"]
            actions = [
                {"add": {"index": index, "alias": alias, "is_write_index": False}}
                for index in context["error"]
            ]
            print(f"Updating {context['tenant']} alias (dry-run={context['dry_run']})\n{json.dumps(actions, indent=2)}"
            )
            if not context["dry_run"]:
                response = update_aliases(context=context, actions=actions)
                print(f"Response:\n{json.dumps(response, indent=2)}")

            return success(context=context, stage=stage)

        return fix(context=verify(context=fetch(context=upstream)))

    initial_context = Context(
        tenant="{{ params.tenant }}",
        tenant_id="{{ params.tenant_id }}",
        retention="{{ params.retention }}",
        conn_id="{{ params.db_conn }}",
        dry_run="{{ params.dry_run }}",
    )
    read_alias(upstream=initial_context)


fix_aliases_dag()

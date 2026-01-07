import json
from collections import defaultdict
from typing import List

from airflow.decorators import dag, task, task_group
from airflow.models import Param

from repo.fix_and_verify import (
    INDEX_REGEX,
    Context,
    success,
    failure,
    chain_on_error_in_stage,
    index_has_expired, extract_index_details,
)
from repo.utils import http_hook_post, http_hook_get


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
            aliased = fetch_indices(prefix=tenant, conn_id=context["conn_id"])
            return success(context=context, stage=stage, value={
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
            print(f"Updating {context['tenant']} alias (dry-run={context['dry_run']})\n{json.dumps(actions, indent=2)}")
            if not context["dry_run"]:
                response = update_aliases(context=context, actions=actions)
                print(f"Response:\n{json.dumps(response, indent=2)}")

            return success(context=context, stage=stage)

        return fix(context=verify(context=fetch(context=upstream)))

    @task_group
    def write_alias(upstream: Context) -> Context:
        stage = "write_alias"
        @task
        def fetch(context: Context) -> Context:
            tenant: str = context["tenant"]
            results = http_hook_get(
                conn_id=context["conn_id"], endpoint=f'/{tenant}/_alias'
            )
            active_aliases = defaultdict(list)
            retention = context["retention"]

            for index, r in results.items():
                details = extract_index_details(index)
                alias = details["write_alias"]
                if index_has_expired(index=index, retention=retention):
                    continue

                aliased = alias in r["aliases"]
                active_aliases[alias].append({
                    "index": index,
                    "aliased": aliased,
                    "is_write_index": False if not aliased else r["aliases"][alias]["is_write_index"],
                })

            return success(context=context, stage=stage, value=active_aliases)

        @task
        def verify(context: Context) -> Context:
            missing = {}
            for alias, indices in context["value"].items():
                if any(i["aliased"] is False for i in indices) or not any(
                    i["is_write_index"] is True for i in indices
                ):
                    missing[alias] = indices
            if missing:
                return failure(context=context, stage=stage, error=missing)
            return success(context=context, stage=stage)

        @task
        @chain_on_error_in_stage(stage=stage)
        def fix(context: Context) -> Context:
            actions = []
            for alias, indices in context["error"].items():
                if any(i["is_write_index"] is True for i in indices):
                    write_index = [i["index"] for i in indices if i["is_write_index"] is True][0]
                else:
                    write_index = max(
                        [i["index"] for i in indices],
                        key=lambda i: extract_index_details(i)["suffix"],
                    )
                actions += [
                    {
                        "add": {
                            "index": i["index"],
                            "alias": alias,
                            "is_write_index": write_index == i["index"],
                        }
                    }
                    for i in indices
                ]
            print(f"Updating {context['tenant']} alias (dry-run={context['dry_run']})\n{json.dumps(actions, indent=2)}")
            if not context["dry_run"]:
                response = update_aliases(context=context, actions=actions)
                print(f"Response:\n{json.dumps(response, indent=2)}")

            return success(context=context, stage=stage)

        return fix(context=verify(context=fetch(context=upstream)))

    @task_group
    def rollover_alias(upstream: Context) -> Context:
        stage = "rollover_alias"
        @task
        def fetch(context: Context) -> Context:
            tenant: str = context["tenant"]
            read = fetch_indices(prefix=tenant, conn_id=context["conn_id"])

            results = http_hook_get(context["conn_id"], f"/_cat/aliases/{tenant}-rollover")
            rollover = {r["index"]: r["is_write_index"] == "true" for r in results if INDEX_REGEX.match(r["index"])}

            return success(context=context, stage=stage, value={
                "read": read,
                "rollover": rollover,
            })

        @task
        def verify(context: Context) -> Context:
            needs_fixing = []
            read = context["value"]["read"]
            rollover = context["value"]["rollover"]
            for index in read:
                if index not in rollover:
                    needs_fixing.append(index)
                    continue

                details = extract_index_details(index)
                if rollover[index] != details["should_rollover"]:
                    needs_fixing.append(index)

            if needs_fixing:
                return failure(context=context, stage=stage, error=needs_fixing)
            return success(context=context, stage=stage)

        @task
        @chain_on_error_in_stage(stage=stage)
        def fix(context: Context) -> Context:
            alias = f"{context['tenant']}-rollover"
            actions = [
                {
                    "add": {
                        "index": index,
                        "alias": alias,
                        "is_write_index": extract_index_details(index)[
                            "should_rollover"
                        ],
                    }
                }
                for index in context["error"]
            ]

            print(f"Updating {context['tenant']} alias (dry-run={context['dry_run']})\n{json.dumps(actions, indent=2)}")
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
    rollover_alias(upstream=write_alias(upstream=read_alias(upstream=initial_context)))


fix_aliases_dag()

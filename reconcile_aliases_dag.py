import json
from collections import defaultdict
from typing import List

from airflow.decorators import dag, task
from airflow.models import Param

from fix_and_verify import (
    INDEX_REGEX, generate_write_aliases,
)
from utils import (
    http_hook_post,
    http_hook_get,
    Context,
    success, param_value,
)


def fetch_indices(prefix: str, conn_id: str) -> List[str]:
    results = http_hook_get(conn_id, f"/_cat/indices/{prefix}?h=index&format=json")
    return [r["index"] for r in results if INDEX_REGEX.match(r["index"])]


def update_aliases(conn_id, actions):
    return http_hook_post(
        conn_id,
        "/_aliases/",
        json.dumps({"actions": actions}),
    )


def get_sorted_aliases(conn_id: str, endpoint: str):
    return http_hook_get(
        conn_id,
        endpoint,
        params={"s": "index", "format": "json", "h": "alias,index,is_write_index"},
    )


@dag(
    dag_display_name="Reconcile Aliases",
    tags=["spf", "elasticsearch"],
    description="This DAG replaces fix and verify job.",
    schedule=None,
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
def reconcile_aliases_dag():
    stage = "reconcile_aliases"

    @task
    def reconcile(context: Context) -> Context:
        dry_run = param_value("dry_run")
        conn_id = param_value("conn_id")
        tenant = context["tenant"]
        actions = []

        read_alias = [r["index"] for r in get_sorted_aliases(conn_id, f"/_cat/aliases/{tenant}")]

        write_alias = defaultdict(list)
        for r in get_sorted_aliases(conn_id, f"/_cat/aliases/{tenant}-20*"):
            write_alias[r["alias"]].append((r["index"], r["is_write_index"]))

        for monthly_alias in generate_write_aliases(
                context["tenant"], context["retention"]
        ):
            if not any(is_write_index == "true" for _, is_write_index in write_alias.get(monthly_alias)):
                latest_index = write_alias.get(monthly_alias)[-1][0]
                actions.append({
                    "add": {"index": latest_index, "alias": monthly_alias, "is_write_index": True}
                })

            for index, _ in write_alias.get(monthly_alias):
                if not index in read_alias:
                    actions.append({
                        "add": {"index": index, "alias": tenant, "is_write_index": False}
                    })

        rollover = get_sorted_aliases(conn_id, f"/_cat/aliases/{tenant}-rollover")[-1]
        if not rollover["is_write_index"] == "true":
            actions.append({
                "add": {"index": f"seaas-{tenant}-*", "alias": f"{tenant}-rollover", "is_write_index": False}
            })
            actions.append({
                "add": {"index": rollover["index"], "alias": f"{tenant}-rollover", "is_write_index": True}
            })

        if actions and not dry_run:
            response = update_aliases(conn_id, actions=actions)
            print(f"Response:\n{json.dumps(response, indent=2)}")

        return success(
            context=context,
            stage=stage,
            value=actions)

    initial_context = Context(
        tenant="{{ params.tenant }}",
        tenant_id="{{ params.tenant_id }}",
        retention="{{ params.retention }}"
    )

    reconcile(context=initial_context)


reconcile_aliases_dag()

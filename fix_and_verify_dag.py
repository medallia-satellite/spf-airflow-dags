from collections import defaultdict
from typing import List, Tuple

from airflow.decorators import dag, task_group, task
from airflow.models import Param
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from fix_and_verify import (
    BASE_REGEX,
    INDEX_REGEX,
    ALIAS_REGEX_MAPPING,
    POLICY_MAPPING,
    expected_index_template,
    extract_index_details,
    generate_write_aliases,
)
from utils import (
    xcom_pull,
    xcom_push,
    http_hook_get,
    chain_on_success,
    Context,
    success,
    failure,
    wait_for_completion,
    select_eligible_for_fix,
)



def fetch_indices_in_alias(alias: str, conn_id: str) -> List[Tuple[str, str, bool]]:
    results = http_hook_get(conn_id, f"/_cat/aliases/{alias}")
    return [(i["index"], i["alias"], i["is_write_index"] == "true") for i in results]


def fetch_indices(prefix: str, conn_id: str) -> List[str]:
    results = http_hook_get(conn_id, f"/_cat/indices/{prefix}*?h=index&format=json")
    return [r["index"] for r in results]


@dag(
    dag_display_name="Fix & Verify",
    tags=["spf", "elasticsearch"],
    description="This DAG replaces fix and verify job.",
    max_active_runs=1,
    schedule=None,
    catchup=False,
    params={
        "conn_id": Param("es-testing", type="string"),
        "dry_run": Param(True, type="boolean"),
    },
    render_template_as_native_obj=True,
)
def fix_and_verify_dag():
    @task
    def fetch_indices_per_tenant() -> List[Context]:

        fetched = http_hook_get(get_param("conn_id"), "/_cat/indices?h=index&format=json")
        results = defaultdict(list)
        for index in [r["index"] for r in fetched if INDEX_REGEX.match(r["index"])]:
            results[
                (
                    BASE_REGEX.search(index).group(0),
                    int(INDEX_REGEX.fullmatch(index).groupdict()["tenant_id"]),
                )
            ].append(index)

        grouped = []
        for k, v in results.items():
            xcom_push(k[0], v)
            latest_suffix = max([extract_index_details(i)["suffix"] for i in v])
            grouped.append(
                Context(
                    success=True,
                    tenant=k[0],
                    tenant_id=k[1],
                    latest_suffix=latest_suffix
                )
            )
        return grouped

    @task_group
    def ilm_settings(upstream: List[Context]) -> List[Context]:
        stage = "ilm_settings"

        @task
        def fetch(data: List[Context]) -> List[Context]:
            results = http_hook_get(
                get_param("conn_id"),
                "/_settings/index.lifecycle.name,index.lifecycle.rollover_alias",
            )
            for k, v in results.items():
                if INDEX_REGEX.match(k):
                    xcom_push(k, v)
            return data

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            indices = xcom_pull("fetch_indices_per_tenant", context["tenant"])
            il_list = [
                s["settings"]["index"]["lifecycle"]
                for index in indices
                if (s := xcom_pull("ilm_settings.fetch", index))
            ]

            if len(indices) != len(il_list):
                return failure(
                    context=context,
                    stage=stage,
                    error="Some indices are missing ILM settings",
                )

            policies = [il.get("name") for il in il_list]
            rollover = [il.get("rollover_alias") for il in il_list]

            if (
                len(set(POLICY_MAPPING.get(p) for p in policies)) != 1
                or policies[0] not in POLICY_MAPPING
            ):
                return failure(
                    context=context,
                    stage=stage,
                    error=f"Invalid policies: {set(policies)}",
                )

            if not all(r == f"{context['tenant']}-rollover" for r in rollover):
                return failure(
                    context=context,
                    stage=stage,
                    error=f"Invalid rollover alias {rollover}",
                )

            context.update({"retention": POLICY_MAPPING[policies[0]]})
            return success(context=context, stage=stage)

        verified = verify.expand(context=fetch(upstream))
        select_eligible_for_fix(upstream=verified, stage=stage)
        return verified

    @task_group
    def index_templates(upstream: List[Context]) -> List[Context]:
        stage = "index_templates"

        @task
        def fetch(data: List[Context]) -> List[Context]:
            results = http_hook_get(get_param("conn_id"), "/_index_template/*-rollover")
            for r in results["index_templates"]:
                if ALIAS_REGEX_MAPPING["rollover"].match(r["name"]):
                    xcom_push(r["name"], r["index_template"])
            return data

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            index_template = xcom_pull(
                "index_templates.fetch", f'{context["tenant"]}-rollover'
            )
            comparable = {k: v for k, v in index_template.items() if k != "composed_of"}
            if comparable != expected_index_template(
                tenant=context["tenant"], retention_months=context["retention"]
            ):
                return failure(
                    context=context,
                    stage=stage,
                    error=f"Invalid index template: {comparable}",
                )

            return success(context=context, stage=stage)

        verified = verify.expand(context=fetch(upstream))
        select_eligible_for_fix(upstream=verified, stage=stage)
        return verified

    @task_group
    def monthly_indices(upstream: List[Context]) -> List[Context]:
        stage = "monthly_indices"

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            indices = xcom_pull("fetch_indices_per_tenant", context["tenant"])
            for write_alias in generate_write_aliases(
                context["tenant"], context["retention"]
            ):
                if not any(
                    index.startswith(f"seaas-{write_alias}") for index in indices
                ):
                    return failure(context=context, stage=stage)
            return success(context=context, stage=stage)

        @task
        def trigger_fix_monthly(context: Context):
            conf = {
                "conn_id": get_param("conn_id"),
                "dry_run": get_param("dry_run"),
                **context
            }
            return TriggerDagRunOperator(
                task_id=f"trigger_monthly_{context['tenant']}",
                trigger_dag_id="fix_monthly_indices_dag",
                wait_for_completion=True,
                conf=conf,
            )


        # trigger_child = TriggerDagRunOperator.partial(
        #     task_id="trigger_fix_monthly_indices_dag",
        #     trigger_dag_id="fix_monthly_indices_dag",  # The DAG ID to trigger
        #     wait_for_completion=True,  # Wait for the child DAG to finish
        #     poke_interval=15,
        #     conf={"conn_id": cfg["conn_id"], "dry_run": cfg["dry_run"]},
        # ).expand(conf=select_eligible_for_fix(upstream=verified, stage=stage))

        verified = verify.expand(context=upstream)
        eligible = select_eligible_for_fix(upstream=verified, stage=stage)
        trigger_child = trigger_fix_monthly.expand(context=eligible)
        t = wait_for_completion(upstream=verified)
        trigger_child >> t
        return t

    @task_group
    def aliases(upstream: Context) -> List[Context]:
        stage = "aliases"

        @task
        @chain_on_success
        def fetch(context: Context) -> Context:
            tenant = context["tenant"]
            indices = fetch_indices(
                prefix=f"seaas-{tenant}-*", conn_id=get_param("conn_id")
            )
            alias_per_index = {i: [] for i in indices}
            results = http_hook_get(get_param("conn_id"), f"/_cat/aliases/{tenant}*")
            for r in results:
                alias_per_index[r["index"]].append(r["alias"])
            return success(context=context, stage=stage, value=alias_per_index)

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            # Each index should have read, write and rollover aliases
            if any(len(a) != 3 for a in context["value"].values()):
                return failure(context=context, stage=stage)
            return success(context=context, stage=stage)

        @task
        def trigger_fix_aliases(context: Context):
            conf = {
                "conn_id": get_param("conn_id"),
                "dry_run": get_param("dry_run"),
                **context
            }
            return TriggerDagRunOperator(
                task_id=f"trigger_fix_aliases_{context['tenant']}",
                trigger_dag_id="fix_aliases_dag",
                wait_for_completion=True,
                conf=conf,
            )

        #
        # trigger_child = TriggerDagRunOperator.partial(
        #     task_id="trigger_fix_aliases_dag",
        #     trigger_dag_id="fix_aliases_dag",  # The DAG ID to trigger
        #     wait_for_completion=True,  # Wait for the child DAG to finish
        #     poke_interval=15,
        #     conf={"conn_id": cfg["conn_id"], "dry_run": cfg["dry_run"]},
        # ).expand(conf=select_eligible_for_fix(upstream=verified, stage=stage))
        #
        verified = verify.expand(context=fetch.expand(context=upstream))

        eligible = select_eligible_for_fix(upstream=verified, stage=stage)
        trigger_child = trigger_fix_aliases.expand(context=eligible)
        t = wait_for_completion(upstream=verified)
        trigger_child >> t
        return t

    def get_param(param: str) -> str:
        ctx = get_current_context()
        return  ctx["params"][param]


    t1 = fetch_indices_per_tenant()
    t2 = ilm_settings(upstream=t1)
    t3 = index_templates(upstream=t2)
    t4 = monthly_indices(upstream=t3)
    t5 = aliases(upstream=t4)


fix_and_verify_dag()

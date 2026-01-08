import pprint
from collections import defaultdict
from typing import List, Tuple

from airflow.decorators import dag, task_group, task
from airflow.models import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from repo.fix_and_verify import (
    BASE_REGEX,
    INDEX_REGEX,
    ALIAS_REGEX_MAPPING,
    POLICY_MAPPING,
    expected_index_template,
    extract_index_details,
    index_has_expired,
    generate_past_month_starts,
    Context,
    chain_on_success,
    failure,
    success,
)
from repo.utils import xcom_pull, xcom_push, http_hook_get


def fetch_indices_in_alias(alias: str, conn_id: str) -> List[Tuple[str, str, bool]]:
    results = http_hook_get(conn_id, f"/_cat/aliases/{alias}")
    return [(i["index"], i["alias"], i["is_write_index"] == "true") for i in results]


def fetch_indices(prefix: str, conn_id: str) -> List[str]:
    results = http_hook_get(conn_id, f"/_cat/indices/{prefix}*?h=index&format=json")
    return [r["index"] for r in results]


@dag(
    dag_display_name="Verify",
    tags=["spf", "elasticsearch"],
    description="AAAA",
    max_active_runs=1,
    catchup=False,
    params={
        "conn_id": Param("es-testing", type="string"),
        "dry_run": Param(True, type="boolean"),
    },
    render_template_as_native_obj=True,
)
def verify_dag():
    @task(trigger_rule="none_failed")
    def wait_for_completion(upstream: List[Context]) -> List[Context]:
        return upstream

    @task
    def select_eligible_for_fix(upstream: List[Context], stage: str) -> List[Context]:
        eligible_for_fix = []
        for i, c in enumerate(upstream):
            if not c["success"] and c["stage"] == stage:
                print(f"{i}: {c['tenant']}")
                eligible_for_fix.append(c)
        errors = [x for x in upstream if not x["success"]]
        print(
            f"""

            Summary
                success: {len(upstream) - len(errors)}/{len(upstream)}
                errors: {len(errors)}/{len(upstream)}
                errors in stage {stage}: {len([e for e in errors if e["stage"] == stage])}/{len(errors)}
                """
        )
        return eligible_for_fix

    @task
    def report(upstream: List[Context], stage: str) -> List[Context]:
        errors = [x for x in upstream if not x["success"]]
        print(
            f"""
        success: {len(upstream) - len(errors)}/{len(upstream)}
        errors: {len(errors)}/{len(upstream)}
        errors in stage {stage}: {len([e for e in errors if e["stage"] == stage])}/{len(errors)}
        """
        )
        for i, c in enumerate(upstream):
            if not c["success"] and c["stage"] == stage:
                print(f"{i}: {c['tenant']}")
                pprint.pprint(c)
        return errors

    @task
    def fetch_indices_per_tenant(context: Context) -> List[Context]:
        fetched = http_hook_get(context["conn_id"], "/_cat/indices?h=index&format=json")
        print(fetched)
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
                    latest_suffix=latest_suffix,
                    conn_id=context["conn_id"],
                    dry_run=context["dry_run"],
                )
            )
        return grouped

    @task_group
    def ilm_settings(upstream: List[Context]) -> List[Context]:
        tg_stage = "ilm_settings"

        @task
        def fetch(data: List[Context]) -> List[Context]:
            results = http_hook_get(
                data[0]["conn_id"],
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
                    stage=tg_stage,
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
                    stage=tg_stage,
                    error=f"Invalid policies: {set(policies)}",
                )

            if not all(r == f"{context['tenant']}-rollover" for r in rollover):
                return failure(
                    context=context,
                    stage=tg_stage,
                    error=f"Invalid rollover alias {rollover}",
                )

            context.update({"retention": POLICY_MAPPING[policies[0]]})
            return success(context=context, stage=tg_stage)

        verified = verify.expand(context=fetch(upstream))
        report(upstream=verified, stage=tg_stage)
        return verified

    @task_group
    def index_templates(upstream: List[Context]) -> List[Context]:
        tg_stage = "index_templates"

        @task
        def fetch(data: List[Context]) -> List[Context]:
            results = http_hook_get(data[0]["conn_id"], "/_index_template/*-rollover")
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
            _ = index_template.pop("composed_of")

            if index_template != expected_index_template(
                tenant=context["tenant"], retention_months=context["retention"]
            ):
                return failure(
                    context=context,
                    stage=tg_stage,
                    error=f"Invalid index template: {index_template}",
                )

            return success(context=context, stage=tg_stage)

        verified = verify.expand(context=fetch(upstream))
        report(upstream=verified, stage=tg_stage)
        return verified

    @task_group
    def monthly_indices(upstream: List[Context]) -> List[Context]:
        tg_stage = "monthly_indices"

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            indices = xcom_pull("fetch_indices_per_tenant", context["tenant"])
            for month_start in generate_past_month_starts(context["retention"]):
                if not any(
                    index.startswith(
                        f'seaas-{context["tenant"]}-{month_start:%Y-%m-%d}'
                    )
                    for index in indices
                ):
                    return failure(context=context, stage=tg_stage)
            return success(context=context, stage=tg_stage)

        verified = verify.expand(context=upstream)

        trigger_child = TriggerDagRunOperator.partial(
            task_id="trigger_fix_monthly_indices_dag",
            trigger_dag_id="fix_monthly_indices_dag",  # The DAG ID to trigger
            wait_for_completion=True,  # Wait for the child DAG to finish
            poke_interval=15,
        ).expand(conf=select_eligible_for_fix(upstream=verified, stage=tg_stage))
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
            indices = fetch_indices(prefix=f"seaas-{tenant}-*", conn_id=context["conn_id"])
            alias_per_index = {i: [] for i in indices}
            results = http_hook_get(context["conn_id"], f"/_cat/aliases/{tenant}*")
            for r in results:
                alias_per_index[r["index"]].append(r["alias"])
            return success(context=context, stage=stage, value=alias_per_index)

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            if any(len(a) != 3 for a in context["value"].values()):
                return failure(context=context, stage=stage)
            return success(context=context, stage=stage)

        verified = verify.expand(context=fetch.expand(context=upstream))

        trigger_child = TriggerDagRunOperator.partial(
            task_id="trigger_fix_aliases_dag",
            trigger_dag_id="fix_aliases_dag",  # The DAG ID to trigger
            wait_for_completion=True,  # Wait for the child DAG to finish
            poke_interval=15,
        ).expand(conf=select_eligible_for_fix(upstream=verified, stage=stage))
        t = wait_for_completion(upstream=verified)
        trigger_child >> t
        return t

    @task
    def print_errors(upstream: List[Context]) -> None:

        errors = [x for x in upstream if not x["success"]]
        print(
            f"""
        success: {len(upstream) - len(errors)}/{len(upstream)}
        errors: {len(errors)}/{len(upstream)}
        """
        )

        for i, c in enumerate(upstream):
            if not c["success"]:
                print(
                    f"""
                {c["tenant"]} - {c["stage"]}:
                {pprint.pformat(c["error"], indent=2)}
                """
                )

    initial_context = Context(
        conn_id="{{ params.conn_id }}",
        dry_run="{{ params.dry_run }}",
    )
    t1 = fetch_indices_per_tenant(context=initial_context)
    t2 = ilm_settings(upstream=t1)
    t3 = index_templates(upstream=t2)
    t4 = monthly_indices(upstream=t3)
    aliases(upstream=t4)
    # print_errors(upstream=t7)


verify_dag()

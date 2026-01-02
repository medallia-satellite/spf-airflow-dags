import pprint
from collections import defaultdict
from typing import List, Tuple

from airflow.cli.commands.task_command import task_list
from airflow.decorators import dag, task_group, task
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from repo.fix_and_verify import (
    BASE_REGEX,
    INDEX_REGEX,
    ALIAS_REGEX_MAPPING,
    POLICY_MAPPING,
    expected_index_template,
    extract_index_details,
    index_has_expired,
    generate_past_month_starts, Context, chain_on_success, failure, success,
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
        "db_conn": Param("es-testing", type="string"),
        "dry_run": Param(True, type="boolean"),
    },
    render_template_as_native_obj=True,
)
def verify_dag():
    @task(trigger_rule="all_success")
    def wait_for_completion(upstream: List[Context]) -> List[Context]:
        return upstream

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
            il_list = [s["settings"]["index"]["lifecycle"] for index in indices if (s := xcom_pull("ilm_settings.fetch", index))]

            if len(indices) != len(il_list):
                return failure(
                    context=context,
                    stage=tg_stage,
                    error="Some indices are missing ILM settings",
                )

            policies = [il.get("name") for il in il_list]
            rollover = [il.get("rollover_alias") for il in il_list]

            if len(set(POLICY_MAPPING.get(p) for p in policies)) != 1 or policies[0] not in POLICY_MAPPING:
                return failure(
                    context=context,
                    stage=tg_stage,
                    error=f'Invalid policies: {set(policies)}',
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
            task_id='trigger_child_dag',
            trigger_dag_id='fix_monthly_indices_dag',  # The DAG ID to trigger
            wait_for_completion=True,  # Wait for the child DAG to finish
            poke_interval=15,
            # deferrable=True, # Use this for Airflow 2.2+ instead of wait_for_completion for efficiency
            # execution_date='{{ ds }}', # Pass the parent's execution date if needed
        ).expand(conf=report(upstream=verified, stage=tg_stage))
        t = wait_for_completion(upstream=verified)
        trigger_child >> t
        return t

    @task_group
    def expired_indices(upstream: List[Context]) -> List[Context]:
        tg_stage = "expired_indices"
        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            indices = xcom_pull("fetch_indices_per_tenant", context["tenant"])
            expired = []
            for index in indices:
                if index_has_expired(index, context["retention"]):
                    expired.append(index)
            if expired:
                return failure(context=context, stage=tg_stage, error=expired)
            return success(context=context, stage=tg_stage)

        verified = verify.expand(context=upstream)
        report(upstream=verified, stage=tg_stage)
        return verified

    @task_group
    def read_alias(upstream: List[Context]) -> List[Context]:
        tg_stage = "read_alias"

        @task
        @chain_on_success
        def fetch(context: Context) -> Context:
            tenant = context["tenant"]
            return success(
                context=context,
                stage=tg_stage,
                value={
                    "read_indices": [
                        i
                        for i, _, _ in fetch_indices_in_alias(
                            alias=tenant, conn_id=context["conn_id"]
                        )
                    ],
                    "indices": fetch_indices(prefix=tenant, conn_id=context["conn_id"]),
                },
            )

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            indices = set(context["value"]["indices"])
            read_indices = set(context["value"]["read_indices"])
            if len(indices) != len(read_indices):
                return failure(
                    context=context, stage=tg_stage, error=list(indices - read_indices)
                )
            return success(context=context, stage=tg_stage)

        verified = verify.expand(context=fetch.expand(context=upstream))
        report(upstream=verified, stage=tg_stage)
        return verified

    @task_group
    def write_alias(upstream: List[Context]) -> List[Context]:
        tg_stage = "write_alias"

        @task
        @chain_on_success
        def fetch(context: Context) -> Context:
            results = http_hook_get(
                conn_id=context["conn_id"], endpoint=f'/{context["tenant"]}/_alias'
            )
            active_aliases = defaultdict(list)
            retention = context["retention"]

            for index, r in results.items():
                details = extract_index_details(index)
                alias = details["write_alias"]
                if index_has_expired(index=index, retention=retention):
                    continue
                is_write = r["aliases"].get(alias, {}).get("is_write_index")
                active_aliases[alias].append((index, is_write))

            return success(context=context, stage=tg_stage, value=active_aliases)

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            fetched = context["value"]
            missing = {}
            print(fetched)
            for alias, indices in context["value"].items():
                if any(i[1] is None for i in indices) or not any(
                    i[1] is True for i in indices
                ):
                    missing[alias] = indices
            if missing:
                return failure(context=context, stage=tg_stage, error=missing)
            return success(context=context, stage=tg_stage)
        verified = verify.expand(context=fetch.expand(context=upstream))
        report(upstream=verified, stage=tg_stage)
        return verified

    @task_group
    def rollover_alias(upstream: List[Context]) -> List[Context]:
        tg_stage = "rollover_alias"

        @task
        def fetch(context: Context) -> Context:
            if not context["success"]:
                return context
            tenant = context["tenant"]
            return success(
                context=context,
                stage=tg_stage,
                value={
                    "read_alias": [
                        i
                        for i, _, _ in fetch_indices_in_alias(
                            alias=tenant, conn_id=context["conn_id"]
                        )
                    ],
                    "rollover_alias": {
                        i: b
                        for i, _, b in fetch_indices_in_alias(
                            alias=f"{tenant}-rollover", conn_id=context["conn_id"]
                        )
                    },
                },
            )

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            indices = context["value"]
            indices_in_read_alias = indices["read_alias"]
            indices_in_rollover_alias = indices["rollover_alias"]

            needs_fixing = []
            for index in indices_in_read_alias:
                if index not in indices_in_rollover_alias:
                    needs_fixing.append(index)
                    continue
                details = extract_index_details(index)

                if indices_in_rollover_alias[index] and details["should_rollover"]:
                    return success(context=context, stage=tg_stage)

                if indices_in_rollover_alias[index] or details["should_rollover"]:
                    needs_fixing.append(index)

            return failure(context=context, stage=tg_stage, error=needs_fixing)

        verified = verify.expand(context=fetch.expand(context=upstream))
        report(upstream=verified, stage=tg_stage)
        return verified

    @task
    def print_errors(upstream: List[Context]) -> None:

        errors = [x for x in upstream if not x["success"]]
        print(f"""
        success: {len(upstream) - len(errors)}/{len(upstream)}
        errors: {len(errors)}/{len(upstream)}
        """
        )

        for i, c in enumerate(upstream):
            if not c["success"]:
                print(f"""
                {c["tenant"]} - {c["stage"]}:
                {pprint.pformat(c["error"], indent=2)}
                """)

    initial_context = Context(
        conn_id="{{ params.db_conn }}",
        dry_run="{{ params.dry_run }}",
    )
    t1 = fetch_indices_per_tenant(context=initial_context)
    t2 = ilm_settings(upstream=t1)
    t3 = index_templates(upstream=t2)
    t4 = monthly_indices(upstream=t3)
    te = expired_indices(upstream=t3)
    t5 = read_alias(upstream=t4)
    t6 = write_alias(upstream=t5)
    t7 = rollover_alias(upstream=t6)
    print_errors(upstream=t7)

verify_dag()

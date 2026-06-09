import json
import logging
from collections import defaultdict
from typing import List

from airflow.decorators import dag, task_group, task
from airflow.models import Param

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
    select_eligible_for_fix, param_value, http_hook_put, http_hook_post,
)



@dag(
    dag_display_name="Reconcile Wordtags Indices",
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
def reconcile_wordtags_indices_dag():
    @task
    def fetch_indices_per_tenant() -> List[Context]:
        conn_id = param_value("conn_id")

        fetched = http_hook_get(
            conn_id,
            "/_cat/indices",
            params={
                "s": "index",
                "h": "index",
                "format": "json",
            }
        )

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
            conn_id = param_value("conn_id")

            results = http_hook_get(
                conn_id,
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

            policies = [il.get("name") for il in il_list]
            rollover = [il.get("rollover_alias") for il in il_list if il.get("rollover_alias")]

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

            if len(indices) != len(il_list):
                return failure(
                    context=context,
                    stage=stage,
                    error="Some indices are missing ILM settings",
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
            conn_id = param_value("conn_id")

            results = http_hook_get(conn_id, "/_index_template/*-rollover")
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
            if not index_template:
                return failure(
                    context=context,
                    stage=stage,
                    error=f"Index template not found",
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

        @task
        def fix(context: Context) -> Context:
            conn_id = param_value("conn_id")
            dry_run = param_value("dry_run")

            template_name = f"{context['tenant']}-rollover"
            index_template = expected_index_template(
                    tenant=context["tenant"],
                    retention_months=context["retention"],
                )

            print(
                f"Creating Index template: {template_name} (dry-run={dry_run})\n{json.dumps(index_template, indent=2)}"
            )

            if not dry_run:
                response = http_hook_put(
                    conn_id,
                    f"_index_template/{template_name}",
                    json.dumps(index_template)
                )
                print(f"Response:\n{json.dumps(response, indent=2)}")


            return success(context=context, stage=stage)

        verified = verify.expand(context=fetch(upstream))
        eligible = select_eligible_for_fix(upstream=verified, stage=stage)
        fixed = fix.expand(context=eligible)
        t = wait_for_completion(upstream=verified)
        fixed >> t
        return t

    @task_group
    def monthly_indices(upstream: List[Context]) -> List[Context]:
        stage = "monthly_indices"

        @task
        @chain_on_success
        def reconcile(context: Context) -> Context:
            conn_id = param_value("conn_id")
            dry_run = param_value("dry_run")

            suffix = context["latest_suffix"]

            indices = xcom_pull("fetch_indices_per_tenant", context["tenant"])
            missing = []
            for write_alias in generate_write_aliases(
                context["tenant"], context["retention"]
            ):
                if not any(
                    index.startswith(f"seaas-{write_alias}") for index in indices
                ):
                    missing.append(write_alias)

            for write_alias in missing:
                suffix += 1
                index = f'seaas-{write_alias}-{context["tenant_id"]}-{suffix:06}'
                details = extract_index_details(index)
                policy = f'M{context["retention"]}_rollover' if details["should_rollover"] else f'M{context["retention"]}'
                payload = {
                    "settings": {
                        "index.lifecycle.origination_date": details["origination_date"],
                        "index.lifecycle.name": policy,
                    },
                    "aliases": {
                        details["read_alias"]: {"is_write_index": False},
                        details["write_alias"]: {"is_write_index": True},
                        details["rollover_alias"]: {"is_write_index": False},
                    },
                }
                print(
                    f"Creating index: {index} (dry-run={dry_run})\n{json.dumps(payload, indent=2)}"
                )
                if not dry_run:
                    response = http_hook_put(conn_id, index, json.dumps(payload))
                    print(f"Response:\n{json.dumps(response, indent=2)}")

            return success(context=context, stage=stage, value=missing)

        @task
        def report(contexts: List[Context]) -> None:
            for i, c in enumerate(contexts):
                if c["value"] and c["stage"] == stage:
                    logging.info(f"{i}: {c['tenant']}\n{c['value']}")


        r = reconcile.expand(context=upstream)
        report(r)

        return r

    @task_group
    def aliases(upstream: Context) -> List[Context]:
        def fetch_aliases(conn_id: str, endpoint: str):
            return http_hook_get(
                conn_id,
                endpoint,
                params={
                    "s": "index",
                    "format": "json",
                    "h": "alias,index,is_write_index"
                },
            )

        stage = "aliases"

        @task
        @chain_on_success
        def reconcile(context: Context) -> Context:
            dry_run = param_value("dry_run")
            conn_id = param_value("conn_id")
            tenant = context["tenant"]
            actions = []

            read_alias = [r["index"] for r in fetch_aliases(conn_id, f"/_cat/aliases/{tenant}")]

            write_alias = defaultdict(list)
            for resp in fetch_aliases(conn_id, f"/_cat/aliases/{tenant}-20*"):
                write_alias[resp["alias"]].append((resp["index"], resp["is_write_index"]))

            for monthly_alias in generate_write_aliases(
                    context["tenant"], context["retention"]
            ):
                if monthly_alias not in write_alias:
                    continue

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

            rollover = fetch_aliases(conn_id, f"/_cat/aliases/{tenant}-rollover")[-1]
            if not rollover["is_write_index"] == "true":
                actions.append({
                    "add": {"index": f"seaas-{tenant}-*", "alias": f"{tenant}-rollover", "is_write_index": False}
                })
                actions.append({
                    "add": {"index": rollover["index"], "alias": f"{tenant}-rollover", "is_write_index": True}
                })

            if actions and not dry_run:
                response = http_hook_post(conn_id, "/_aliases/", json.dumps({"actions": actions}))
                print(f"Response:\n{json.dumps(response, indent=2)}")

            return success(
                context=context,
                stage=stage,
                value=actions,
            )

        @task
        def report(contexts: List[Context]) -> None:
            for i, c in enumerate(contexts):
                if c["value"] and c["stage"] == stage:
                    logging.info(f"{i}: {c['tenant']}\n{c['value']}")


        r = reconcile.expand(context=upstream)
        report(r)

        return r

    t1 = fetch_indices_per_tenant()
    t2 = ilm_settings(upstream=t1)
    t3 = index_templates(upstream=t2)
    t4 = monthly_indices(upstream=t3)
    t5 = aliases(upstream=t4)


reconcile_wordtags_indices_dag()

import json
from collections import defaultdict
from typing import List

from airflow.decorators import dag, task_group, task
from airflow.models import Param

from fix_and_verify import (
    BASE_REGEX,
    INDEX_REGEX,
    POLICY_MAPPING,
    extract_index_details,
    index_has_expired,
)
from utils import (
    xcom_pull,
    xcom_push,
    http_hook_get,
    http_hook_put,
    chain_on_success,
    chain_on_error_in_stage,
    Context,
    success,
    failure,
    select_eligible_for_fix, param_value,
)


def update_index_settings(conn_id: str, index, payload):
    return http_hook_put(conn_id, f"{index}/_settings", json.dumps(payload))


@dag(
    dag_display_name="Finalize Expired Indices",
    tags=["spf", "elasticsearch"],
    description="This DAG marks expired indices as indexing complete to unblock ILM retention lifecycle. This is a one-time fix for expired indices that were not marked as indexing complete.",
    max_active_runs=1,
    schedule=None,
    catchup=False,
    params={
        "conn_id": Param("es-testing", type="string"),
        "dry_run": Param(True, type="boolean"),
    },
    render_template_as_native_obj=True,
)
def finalize_expired_indices_dag():
    @task
    def fetch_indices_per_tenant() -> List[Context]:
        fetched = http_hook_get(
            param_value("conn_id"),
            "/_cat/indices",
            params={"h": "index", "format": "json"},
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
                    latest_suffix=latest_suffix,
                )
            )
        return grouped

    @task_group
    def ilm_settings(upstream: List[Context]) -> List[Context]:
        stage = "ilm_settings"

        @task
        def fetch(data: List[Context]) -> List[Context]:
            results = http_hook_get(
                param_value("conn_id"),
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

            if not rollover or not all(r == f"{context['tenant']}-rollover" for r in rollover):
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
    def expired_indices(upstream: List[Context]) -> List[Context]:
        stage = "expired_indices"

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            indices = xcom_pull("fetch_indices_per_tenant", context["tenant"])
            expired = []
            for index in indices:
                if index_has_expired(index, context["retention"]):
                    expired.append(index)
            if expired:
                return failure(context=context, stage=stage, error=expired)
            return success(context=context, stage=stage)

        @task(max_active_tis_per_dagrun=50)
        @chain_on_error_in_stage(stage=stage)
        def fix(context: Context) -> Context:
            # Marking indexing as completed unblocks ILMs retention lifecycle when rollovers are performed manually.
            for index in context["error"]:
                print(
                    f"Marking indexing as completed (dry-run={param_value('dry_run')}): {index}"
                )
                if param_value("dry_run"):
                    continue

                details = extract_index_details(index)
                response = update_index_settings(
                    param_value("conn_id"),
                    index,
                    {
                        "index.lifecycle.indexing_complete": True,
                        "index.lifecycle.origination_date": details["origination_date"]
                    },
                )
                print(f"Response:\n{json.dumps(response, indent=2)}")
            return success(context=context, stage=stage)

        verified = verify.expand(context=upstream)
        fix.expand(context=select_eligible_for_fix(upstream=verified, stage=stage))
        return verified

    t1 = fetch_indices_per_tenant()
    t2 = ilm_settings(upstream=t1)
    t3 = expired_indices(upstream=t2)


finalize_expired_indices_dag()

import json
import pprint
from collections import defaultdict
from typing import List

from airflow.decorators import dag, task_group, task
from airflow.models import Param

from repo.fix_and_verify import (
    BASE_REGEX,
    INDEX_REGEX,
    POLICY_MAPPING,
    extract_index_details,
    index_has_expired,
    Context,
    chain_on_success,
    failure,
    success, chain_on_error_in_stage,
)
from repo.utils import xcom_pull, xcom_push, http_hook_get, http_hook_put


def fetch_indices(prefix: str, conn_id: str) -> List[str]:
    results = http_hook_get(conn_id, f"/_cat/indices/{prefix}*?h=index&format=json")
    return [r["index"] for r in results]

def update_index_settings(conn_id: str, index, payload):
    return http_hook_put(conn_id, f"{index}/_settings", json.dumps(payload))


@dag(
    dag_display_name="es_index_deletion_unblocker_dag",
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
def es_index_deletion_unblocker_dag():
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


    @task
    @chain_on_success
    def verify(context: Context) -> Context:
        indices = xcom_pull("fetch_indices_per_tenant", context["tenant"])
        expired = []
        for index in indices:
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
        return success(context=context, stage="verify")


    initial_context = Context(
        conn_id="{{ params.conn_id }}",
        dry_run="{{ params.dry_run }}",
    )
    t1 = fetch_indices_per_tenant(context=initial_context)
    t2 = ilm_settings(upstream=t1)
    te = fix.expand(context=verify.expand(context=t2))
    report(upstream=te, stage="verify")
es_index_deletion_unblocker_dag()

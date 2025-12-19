import functools
import json
import pprint
from collections import defaultdict
from typing import TypedDict, Optional, Any, List

from airflow.decorators import dag, task_group, task
from airflow.models import Param
from airflow.operators.python import get_current_context
from airflow.providers.http.hooks.http import HttpHook

from repo.fix_and_verify import *

def chain_on_success(func):
    @functools.wraps(func)
    def wrapper(context):
        if not context["success"]:
            return context
        return func(context)
    return wrapper

def xcom_pull(task_id: str, key: str) -> Any:
    context = get_current_context()
    ti = context["ti"]
    print(f"xcom_pull {task_id} {key}")
    return ti.xcom_pull(task_ids=task_id, key=key)

def xcom_push(key: str, value: Any) -> None:
    context = get_current_context()
    ti = context["ti"]
    print(f"xcom_push {key} {value}")
    ti.xcom_push(key, value)

def generate_past_month_starts(n):
    current_month_start = (datetime.datetime.today()
                           .replace(day=1, hour=0, minute=0, second=0, tzinfo=datetime.timezone.utc)
                           + relativedelta(months=1))
    return [current_month_start - relativedelta(months=i) for i in range(n)]


def http_hook_put(conn_id: str, endpoint: str, data: str):
    hook_put = HttpHook(method='PUT', http_conn_id=conn_id)
    response = hook_put.run(
        endpoint=f'/{endpoint}?pretty',
        headers={'Content-Type': 'application/json'},
        data=data
    )
    hook_put.check_response(response)
    return response.json()

def http_hook_get(conn_id: str, endpoint: str):
    hook_get = HttpHook(method='GET', http_conn_id=conn_id)
    response = hook_get.run(
        endpoint=endpoint,
        headers={'Accept': 'application/json'},
    )
    hook_get.check_response(response)
    return response.json()

class Context(TypedDict, total=False):
    tenant: str
    tenant_id: int
    success: bool
    error: Optional[str]
    stage: Optional[str]
    value: Optional[Any]
    retention: Optional[int]

def success(context: Context, stage: str, value: Any = None) -> Context:
    return Context(
        tenant=context["tenant"],
        tenant_id=context["tenant_id"],
        success=True,
        stage=stage,
        value=value if value else context.get("value"),
        retention=context.get("retention"),
    )

def failure(context: Context, stage: str, error: Any = None) -> Context:
    return Context(
        tenant=context["tenant"],
        tenant_id=context["tenant_id"],
        success=False,
        stage=stage,
        error=error if error else context.get("error"),
        retention=context.get("retention"),
    )




@dag(
    dag_display_name="WIP2",
    tags=["spf", "test", "poc"],
    description="This DAG verifies monthly indices.",
    max_active_runs=1,
    catchup=False,
    params={"db_conn": Param("es-testing", type="string")},

)
def wiwip_dag():
    def _filter_and_push(results, filter_fn) -> None:
        for k, v in results.items():
            if filter_fn(k):
                xcom_push(k, v)

    @task
    def report(upstream: List[Context]) -> List[Context]:
        errors = [x for x in upstream if not x["success"]]
        print(f"""
        success: {len(upstream) - len(errors)}/{len(upstream)}
        errors: {len([x for x in upstream if not x["success"]])}/{len(upstream)}:
        """)
        for e in errors:
            pprint.pprint(e)

        return upstream

    @task
    def extract_errors(upstream: List[Context], stage: str) -> List[Context]:
        return [c for c in upstream if not c["success"] and c["stage"] == stage]

    @task
    def fetch_indices_per_tenant(conn_id: str, stage: str = "") -> List[Context]:
        fetched = http_hook_get(conn_id, "/_cat/indices?h=index&format=json")
        print(fetched)
        results = defaultdict(list)
        for index in [r["index"] for r in fetched if INDEX_REGEX.match(r["index"])]:
            results[
                (BASE_REGEX.search(index).group(0), INDEX_REGEX.fullmatch(index).groupdict()["tenant_id"])].append(
                index)

        grouped = []
        for k, v in results.items():
            xcom_push(k[0], v)
            grouped.append(Context(tenant=k[0], tenant_id=k[1], stage=stage, success=True))
        return grouped

    @task_group
    def ilm_settings(conn_id: str, upstream: List[Context]) -> List[Context]:
        tg_stage = "ilm_settings"
        @task
        def fetch(h: str) -> bool:
            results = http_hook_get(h, "/_settings/index.lifecycle.name,index.lifecycle.rollover_alias")
            _filter_and_push(results, lambda x: INDEX_REGEX.match(x))
            return True

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            indices = xcom_pull("fetch_indices_per_tenant", context["tenant"])
            il_list = []
            for index in indices:
                if s := xcom_pull("ilm_settings.fetch", index):
                    il_list.append(s["settings"]["index"]["lifecycle"])
                else:
                    print(f"No settings for {index}")

            if len(indices) != len(il_list):
                return failure(context=context, stage=tg_stage, error="Some indices are missing ILM settings")

            retention = list(set(POLICY_MAPPING.get(il.get("name")) for il in il_list))
            if len(retention) != 1 or retention[0] not in POLICY_MAPPING.values():
                return failure(context=context, stage=tg_stage, error=f'Invalid policies: {set(il.get("name") for il in il_list)}')

            rollover = set(il.get("rollover_alias") for il in il_list)
            if not all(r == f"{context['tenant']}-rollover" for r in rollover):
                return failure(context=context, stage=tg_stage, error=f"Invalid rollover alias {rollover}")

            return Context(tenant=context["tenant"], tenant_id=context["tenant_id"], stage=tg_stage, success=True, retention=retention[0])

        f = fetch(conn_id)
        v = verify.expand(context=upstream)
        f >> v
        return v

    @task_group
    def index_templates(conn_id: str, upstream: List[Context]) -> List[Context]:
        tg_stage = "index_templates"
        @task
        def fetch(h: str) -> bool:
            results = http_hook_get(h, "/_index_template/*-rollover")
            _filter_and_push(
                {r["name"]: r["index_template"] for r in results["index_templates"]},
                lambda x: ALIAS_REGEX_MAPPING["rollover"].match(x),
            )
            return True

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            index_template = xcom_pull("index_templates.fetch", f'{context["tenant"]}-rollover')
            _ = index_template.pop("composed_of")

            if index_template != expected_index_template(tenant=context["tenant"], retention_months=context["retention"]):
                return failure(context=context, stage=tg_stage, error=f"Invalid index template: {index_template}")

            return success(context=context, stage=tg_stage)

        f = fetch(conn_id)
        v = verify.expand(context=upstream)
        f >> v
        return v

    @task_group
    def monthly_indices(conn_id: str, upstream: List[Context]) -> List[Context]:
        tg_stage = "monthly_indices"

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            indices = xcom_pull("fetch_indices_per_tenant", context["tenant"])
            missing = []
            for month_start in generate_past_month_starts(context["retention"]):
                if not any(f'{context["tenant"]}-{month_start:%Y-%m-%d}' in index for index in indices):
                    missing.append(month_start)
            if missing:
                return failure(context=context, stage=tg_stage, error=missing)
            return success(context=context, stage=tg_stage)


        @task
        def fix(c: str, context: Context) -> Context:
            if context["success"] or context["stage"] != "monthly_indices":
                return context

            for month_start in context["error"]:
                index = f'seaas-{context["tenant"]}-{month_start:%Y-%m-%d}-{context["tenant_id"]}-0'
                aliases = generate_aliases(index)
                origination_date = int(month_start.timestamp() * 1e3)
                payload = {
                    "settings": {"index.lifecycle.origination_date": origination_date},
                    "aliases": {
                        aliases["read"]: {"is_write_index": False},
                        aliases["write"]: {"is_write_index": True},
                        aliases["rollover"]: {"is_write_index": False},
                    }
                }
                response = http_hook_put(c, index, json.dumps(payload))
                print(f"{index}: {response}")

            return success(context=context, stage="add_missing_indices")

        return fix.partial(c=conn_id).expand(context=report(verify.expand(context=upstream)))

    @task_group
    def aliases(conn_id: str, upstream: List[Context]) -> List[Context]:
        tg_stage = "aliases"
        @task
        def fetch(h: str, data: List[Context]) -> List[Context]:
            results = http_hook_get(h, "/_aliases")
            _filter_and_push(results, lambda x: INDEX_REGEX.match(x))
            return data

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            indices = xcom_pull("fetch_indices_per_tenant", context["tenant"])

            active_indices = []
            for month_start in generate_past_month_starts(context["retention"]):
                active_indices += [index for index in indices if f'{context["tenant"]}-{month_start:%Y-%m-%d}' in index]

            for index in active_indices:
                aa = xcom_pull("aliases.fetch", index)
                if not all([any(r.fullmatch(alias) for r in ALIAS_REGEX_MAPPING.values()) for alias in
                            aa["aliases"].keys()]):
                    return failure(context=context, stage=tg_stage, error="Alias not complete")
            return success(context=context, stage=tg_stage)
        v = verify.expand(context=fetch(conn_id, upstream))
        return v

    @task
    def print_all(upstream: List[Context]) -> None:
        for c in upstream:
            pprint.pprint(c, indent=2)
    connection_id = "{{ params.db_conn }}"
    t1 = fetch_indices_per_tenant(conn_id=connection_id)
    t2 = ilm_settings(conn_id=connection_id, upstream=t1)
    t3 = index_templates(conn_id=connection_id, upstream=t2)
    t4 = monthly_indices(conn_id=connection_id, upstream=t3)
    t5 = aliases(conn_id=connection_id, upstream=t4)

    t1 >> t2 >> t3 >> t4 >> t5 >> print_all(upstream=t5)


wiwip_dag()

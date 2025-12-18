import functools
import json
from collections import defaultdict
from typing import TypedDict, Optional, Any, List

from airflow.decorators import dag, task_group, task
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
    current_month_start = datetime.datetime.today().replace(day=1, hour=0, minute=0, second=0, tzinfo=datetime.timezone.utc) + relativedelta(months=1)
    return [current_month_start - relativedelta(months=i) for i in range(n)]

def fetch_from_endpoint(hook: HttpHook, endpoint: str):
    response = hook.run(
        endpoint=endpoint,
        headers={'Accept': 'application/json'},
    )
    hook.check_response(response)
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
)
def wiwip_dag():
    def _filter_and_push(results, filter_fn) -> None:
        for k, v in results.items():
            if filter_fn(k):
                xcom_push(k, v)

    @task_group
    def fetch_and_group_indices(hook: HttpHook) -> List[Context]:
        tg_stage = "fetch_and_group_indices"
        @task
        def fetch(h: HttpHook) -> List[str]:
            results = fetch_from_endpoint(h, "/_cat/indices?h=index&format=json")
            return [r["index"] for r in results if INDEX_REGEX.match(r["index"])]

        @task
        def group_by_tenant(data: list) -> List[Context]:
            results = defaultdict(list)
            for index in data:
                results[(BASE_REGEX.search(index).group(0), INDEX_REGEX.fullmatch(index).groupdict()["tenant_id"])].append(index)

            grouped = []
            for k, v in results.items():
                xcom_push(k[0], v)
                grouped.append(Context(tenant=k[0], tenant_id=k[1], stage=tg_stage, success=True))
            return grouped

        return group_by_tenant(fetch(hook))

    @task_group
    def ilm_settings(hook: HttpHook, upstream: List[Context]) -> List[Context]:
        tg_stage = "ilm_settings"
        @task
        def fetch(h: HttpHook) -> bool:
            results = fetch_from_endpoint(h, "/_settings/index.lifecycle.name,index.lifecycle.rollover_alias")
            _filter_and_push(results, lambda x: INDEX_REGEX.match(x))
            return True

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            indices = xcom_pull("fetch_and_group_indices.group_by_tenant", context["tenant"])
            il_list = []
            for index in indices:
                if s := xcom_pull("ilm_settings.fetch", index):
                    il_list.append(s["settings"]["index"]["lifecycle"])
                else:
                    print(f"No settings for {index}")

            if any("name" not in il for il in il_list):
                return failure(context=context, stage=tg_stage, error=f"Invalid policies: {il_list}")

            policies = set(il["name"] for il in il_list)
            if not all(p in POLICY_MAPPING for p in policies):
                return failure(context=context, stage=tg_stage, error=f"Invalid policies: {policies}")

            if len(set(POLICY_MAPPING.get(p) for p in policies)) != 1:
                return failure(context=context, stage=tg_stage, error=f"Retention period is not unique: {policies}")

            if any("rollover_alias" not in il for il in il_list):
                return failure(context=context, stage=tg_stage,
                               error=f"No rollover alias {[il for il in il_list if 'rollover_alias' not in il]}")

            rollover = set(il["rollover_alias"] for il in il_list)
            if not all(a == f"{context['tenant']}-rollover" for a in rollover):
                return failure(context=context, stage=tg_stage, error=f"Invalid rollover alias {rollover}")

            retention = next(iter(set(POLICY_MAPPING.get(p) for p in policies)))

            return Context(tenant=context["tenant"], stage=tg_stage, success=True, retention=retention)

        f = fetch(hook)
        v = verify.expand(context=upstream)
        f >> v
        return v

    @task_group
    def index_templates(hook: HttpHook, upstream: List[Context]) -> List[Context]:
        tg_stage = "index_templates"
        @task
        def fetch(h: HttpHook) -> bool:
            results = fetch_from_endpoint(h, "/_index_template/*-rollover")
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

        f = fetch(hook)
        v = verify.expand(context=upstream)
        f >> v
        return v

    @task_group
    def monthly_indices(hook: HttpHook, upstream: List[Context]) -> List[Context]:
        tg_stage = "monthly_indices"

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            indices = xcom_pull("fetch_and_group_indices.group_by_tenant", context["tenant"])
            missing = []
            for month_start in generate_past_month_starts(context["retention"]):
                if not any(f'{context["tenant"]}-{month_start:%Y-%m-%d}' in index for index in indices):
                    missing.append(month_start)
            if missing:
                return failure(context=context, stage=tg_stage, error=missing)
            return success(context=context, stage=tg_stage)
        v = verify.expand(context=upstream)
        return v

    @task
    def add_missing_indices(hook: HttpHook, context: Context) -> Context:
        if context["success"] or context["stage"] != "monthly_indices":
            return context

        for month_start in context["error"]:
            index = f'{context["tenant"]}-{month_start:%Y-%m-%d}-{context["tenant_id"]}-0'
            aliases = generate_aliases(index)
            origination_date = month_start.timestamp()
            payload = {
                "settings": {"index.lifecycle.origination_date": origination_date},
                "aliases": {
                    aliases["read"]: {"is_write_index": False},
                    aliases["write"]: {"is_write_index": True},
                    aliases["rollover"]: {"is_write_index": False},
                }
            }
            print(f"Fixing {index}: {payload}")

        return success(context=context, stage="add_missing_indices")

    @task_group
    def aliases(hook: HttpHook, upstream: List[Context]) -> List[Context]:
        tg_stage = "aliases"
        @task
        def fetch(h: HttpHook) -> bool:
            results = fetch_from_endpoint(h, "/_aliases")
            _filter_and_push(results, lambda x: INDEX_REGEX.match(x))
            return True

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            indices = xcom_pull("refetch_and_group_indices.group_by_tenant", context["tenant"])

            active_indices = []
            for month_start in generate_past_month_starts(context["retention"]):
                active_indices += [index for index in indices if f'{context["tenant"]}-{month_start:%Y-%m-%d}' in index]

            for index in active_indices:
                aa = xcom_pull("aliases.fetch", index)
                if not all([any(r.fullmatch(alias) for r in ALIAS_REGEX_MAPPING.values()) for alias in
                            aa["aliases"].keys()]):
                    return failure(context=context, stage=tg_stage, error="Alias not complete")
            return success(context=context, stage=tg_stage)
        f = fetch(hook)
        v = verify.expand(context=upstream)
        f >> v
        return v

    @task
    def print_all(upstream: List[Context]) -> None:
        for c in upstream:
            print(json.dumps(c, indent=2))

    hook_get = HttpHook(method='GET', http_conn_id='es-wordtags')
    t1 = fetch_and_group_indices(hook=hook_get)
    t2 = ilm_settings(hook=hook_get, upstream=t1)
    t3 = index_templates(hook=hook_get, upstream=t2)
    t4 = monthly_indices(hook=hook_get, upstream=t3)
    t4_fixed = add_missing_indices.partial(hook=hook_get).expand(context=t4)
    t5 = fetch_and_group_indices.override(group_id="refetch_and_group_indices")(hook=hook_get)
    t4_fixed >> t5
    t6 = monthly_indices(hook=hook_get, upstream=t4_fixed)
    t5 >> t6
    print_all(upstream=t6)


wiwip_dag()

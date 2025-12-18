from collections import defaultdict

from airflow.decorators import dag, task_group
from airflow.providers.http.hooks.http import HttpHook

from repo.elasticsearch_client import *
from repo.fix_and_verify import *
from repo.utils import *


@dag(
    dag_display_name="WIP2",
    tags=["spf", "test", "poc"],
    description="This DAG verifies monthly indices.",
    max_active_runs=1,
    catchup=False,
)
def wiwip_dag():
    def _filter_and_push(results, filter_fn = lambda _: True) -> Result:
        for k, v in results.items():
            if filter_fn(k):
                xcom_push(k, v)
        return success("all", list(results.keys()))

    @task_group
    def fetch_and_group_indices(hook: HttpHook):
        @task
        def fetch(h: HttpHook) -> List[str]:
            results = fetch_from_endpoint(h, "/_cat/indices?h=index&format=json")
            return [r["index"] for r in results if INDEX_REGEX.match(r["index"])]

        @task
        def group_by_tenant(data: list):
            result = defaultdict(list)
            for index in data:
                result[BASE_REGEX.search(index).group(0)].append(index)
            return _filter_and_push(result)

        return group_by_tenant(fetch(hook))


    @task_group
    def ilm_settings(hook: HttpHook, upstream: List[Result]) -> List[Result]:
        @task
        def fetch(h: HttpHook):
            results = fetch_from_endpoint(h, "/_settings/index.lifecycle.name,index.lifecycle.rollover_alias")
            return _filter_and_push(results, lambda x: INDEX_REGEX.match(x))

        @task
        def verify(data: Result):
            tenant = data["key"]
            indices = xcom_pull("fetch_and_group_indices.group_by_tenant", tenant)
            il_list = []
            for index in indices:
                if s := xcom_pull("ilm_settings.fetch", index):
                    il_list.append(s["settings"]["index"]["lifecycle"])
                else:
                    print(f"No settings for {index}")

            if any("name" not in il for il in il_list):
                return failure(key=tenant, error=f"Invalid policies: {il_list}")

            policies = set(il["name"] for il in il_list)
            if not all(p in POLICY_MAPPING for p in policies):
                return failure(key=tenant, error=f"Invalid policies: {policies}")

            if len(set(POLICY_MAPPING.get(p) for p in policies)) != 1:
                return failure(key=tenant, error=f"Retention period is not unique: {policies}")

            if any("rollover_alias" not in il for il in il_list):
                return failure(key=tenant,
                               error=f"No rollover alias {[il for il in il_list if 'rollover_alias' not in il]}")

            rollover = set(il["rollover_alias"] for il in il_list)
            if not all(ALIAS_REGEX_MAPPING["rollover"].match(a) for a in rollover):
                return failure(key=tenant, error=f"Invalid rollover alias {rollover}")

            if len(rollover) != 1:
                return failure(key=tenant, error=f"Rollover alias is not unique {rollover}")

            xcom_push(key=tenant, value={
                "retention": next(iter(set(POLICY_MAPPING.get(p) for p in policies))),
                "rollover_alias": next(iter(rollover)),
            })

            return success(key=tenant, value=None)


    hook_get = HttpHook(method='GET', http_conn_id='es-wordtags')

    f_tg = fetch(hook=hook_get)
    grouped = group_indices(f_tg)
    validated = validate(grouped)
    fix_ilm_settings(validated)
    fix_index_templates(validated)
    fix_monthly_indices(validated)
    fix_monthly_aliases(validated)
    fix_rollover_aliases(validated)
    return validated

wip_dag()

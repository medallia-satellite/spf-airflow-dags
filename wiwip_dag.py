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
    @task
    def extract_success(data: List[Result]) -> List[Result]:
        return [d for d in data if d["success"]]

    def generate_past_month_starts(n):
        current_month_start = datetime.datetime.today().replace(day=1, hour=0, minute=0, second=0, tzinfo=datetime.timezone.utc) + relativedelta(months=1)
        return [current_month_start - relativedelta(months=i) for i in range(n)]

    def _filter_and_push(results, filter_fn = lambda _: True) -> List[Result]:
        filtered = []
        for k, v in results.items():
            if filter_fn(k):
                xcom_push(k, v)
                filtered.append(success(key=k, value=None))
        return filtered

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
        def verify(data: Result) -> Result:
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

        f = fetch(hook)
        e = extract_success(data=upstream)
        v = verify.expand(data=e)
        f >> v
        return v

    @task_group
    def index_templates(hook: HttpHook, upstream: List[Result]) -> List[Result]:
        @task
        def fetch(h: HttpHook) -> List[Result]:
            results = fetch_from_endpoint(h, "/_index_template/*-rollover")
            return _filter_and_push(
                {r["name"]: r["index_template"] for r in results["index_templates"]},
                lambda x: ALIAS_REGEX_MAPPING["rollover"].match(x),
            )

        @task
        def verify(data: Result) -> Result:
            tenant = data["key"]
            settings = xcom_pull("ilm_settings.verify", tenant)[0]
            index_template = xcom_pull("index_templates.fetch", settings["rollover_alias"])
            _ = index_template.pop("composed_of")

            if index_template != expected_index_template(tenant=tenant, retention_months=settings["retention"]):
                return failure(key=tenant, error=f"Invalid index template: {index_template}")

            xcom_push(key=tenant, value=index_template)
            return success(key=tenant, value=index_template)

        f = fetch(hook)
        e = extract_success(data=upstream)
        v = verify.expand(data=e)
        f >> v
        return v

    @task_group
    def monthly_indices(hook: HttpHook, upstream: List[Result]) -> List[Result]:
        @task
        def verify(data: Result) -> Result:
            tenant = data["key"]
            indices = xcom_pull("fetch_and_group_indices.group_by_tenant", tenant)
            num_months = xcom_pull("ilm_settings.verify", tenant)[0]["retention"]

            for month_start in generate_past_month_starts(num_months):
                if not [index for index in indices if f"{tenant}-{month_start:%Y-%m-%d}" in index]:
                    return failure(key=tenant, error="Missing index")
            return success(key=tenant, value=None)
        e = extract_success(data=upstream)
        v = verify.expand(data=e)
        return v

    @task_group
    def aliases(hook: HttpHook, upstream: List[Result]) -> List[Result]:
        @task
        def fetch(h: HttpHook) -> List[Result]:
            results = fetch_from_endpoint(h, "/_aliases")
            return _filter_and_push(results, lambda x: INDEX_REGEX.match(x))
        @task
        def verify(data: Result) -> Result:
            tenant = data["key"]
            indices = xcom_pull("refetch_and_group_indices.group_by_tenant", tenant)
            num_months = xcom_pull("ilm_settings.verify", tenant)["retention"]

            active_indices = []
            for month_start in generate_past_month_starts(num_months):
                active_indices += [index for index in indices if f"{tenant}-{month_start:%Y-%m-%d}" in index]

            for index in active_indices:
                aa = xcom_pull("aliases.fetch", index)
                if not all([any(r.fullmatch(alias) for r in ALIAS_REGEX_MAPPING.values()) for alias in
                            aa["aliases"].keys()]):
                    return failure(key=tenant, error="Alias not complete")

            return success(key=tenant, value=None)
        f = fetch(hook)
        e = extract_success(data=upstream)
        v = verify.expand(data=e)
        f >> v
        return v
    hook_get = HttpHook(method='GET', http_conn_id='es-wordtags')
    t1 = fetch_and_group_indices(hook=hook_get)
    t2 = ilm_settings(hook=hook_get, upstream=t1)
    t3 = index_templates(hook=hook_get, upstream=t2)
    t4 = monthly_indices(hook=hook_get, upstream=t3)
    t5 = fetch_and_group_indices.override(task_id="refetch_and_group_indices")(hook=hook_get)
    t4 >> t5
    t6 = monthly_indices(hook=hook_get, upstream=t4)
    t5 >> t6


wiwip_dag()

from collections import defaultdict

from airflow.decorators import dag, task_group
from airflow.providers.http.hooks.http import HttpHook

from repo.elasticsearch_client import *
from repo.fix_and_verify import *
from repo.utils import *


@dag(
    dag_display_name="WIP",
    tags=["spf", "test", "poc"],
    description="This DAG verifies monthly indices.",
    max_active_runs=1,
    catchup=False,
)
def wip_dag():
    hook_get = HttpHook(method='GET', http_conn_id='es-wordtags')

    @task(task_id="extract")
    @chain_on_success
    def extract_ilm_setting(data):
        instance = data["key"]
        il_list = [s["settings"]["index"]["lifecycle"] for s in data["value"]]

        if any("name" not in il for il in il_list):
            return failure(key=instance, error=f"Invalid policies: {il_list}")

        policies = set(il["name"] for il in il_list)
        if not all(p in POLICY_MAPPING for p in policies):
            return failure(key=instance, error=f"Invalid policies: {policies}")

        if len(set(POLICY_MAPPING.get(p) for p in policies)) != 1:
            return failure(key=instance, error=f"Retention period is not unique: {policies}")

        if any("rollover_alias" not in il for il in il_list):
            return failure(key=instance, error=f"No rollover alias {[il for il in il_list if 'rollover_alias' not in il]}")

        rollover_aliases = set(il["rollover_alias"] for il in il_list)
        if not all(ALIAS_REGEX_MAPPING["rollover"].match(a) for a in rollover_aliases):
            return failure(key=instance, error=f"Invalid rollover alias {rollover_aliases}")

        if len(rollover_aliases) != 1:
            return failure(key=instance, error=f"Rollover alias is not unique {rollover_aliases}")

        result = {
            "retention": next(iter(set(POLICY_MAPPING.get(p) for p in policies))),
            "rollover_alias": next(iter(rollover_aliases)),
        }

        return success(key=instance, value=result)

    @task_group
    def fetch_and_group_indices():
        @task
        def group_by_tenant(indices: list):
            result = defaultdict(list)
            for index in indices:
                result[BASE_REGEX.search(index).group(0)].append(index)
            return [success(key=k, value=v) for k, v in result.items()]

        fetched = fetch_indices(hook=hook_get)
        grouped = group_by_tenant(fetched)
        return push(grouped)

    @task_group
    def fetch_and_group_aliases():
        @task
        def group_by_index(aliases: list):
            result = defaultdict(list)
            for alias_entry in aliases:
                if match := INDEX_REGEX.match(alias_entry["index"]):
                    result[match.group(0)].append(alias_entry["alias"])
            return [success(key=k, value=v) for k, v in result.items()]

        fetched = fetch_aliases(hook=hook_get)
        grouped = group_by_index(fetched)
        return push(grouped)

    @task_group
    def ilm_settings(data: List[Result]):
        f = fetch_alias_settings.partial(hook=hook_get).expand(data=data)
        e = extract_ilm_setting.expand(data=f)
        return push(e)

    def expected_monthly_aliases(tenant):
        num_months = retrieve("ilm_settings", tenant)["retention"]
        start_date = datetime.date.today().replace(day=1) + relativedelta(months=1)
        return [
            f"{tenant}-{(start_date - relativedelta(months=i)):%Y-%m-%d}"
            for i in range(num_months)
        ]

    @task_group
    def validate_monthly_indices(data: List[Result]):
        @task
        def flatten_results(results: List[List[Result]]) -> List[Result]:
            flattened = [item for sublist in results for item in sublist]
            print(f"Flattened result size: {len(flattened)}")
            print(f"OK monthly indices: {len([r for r in flattened if r['success']])}/{len(flattened)}")
            print(
                f"Missing monthly indices: {len([r for r in flattened if r['error'] == 'Missing index'])}/{len(flattened)}")
            print(
                f"Indices with missing aliases: {len([r for r in flattened if r['error'] == 'Alias not complete'])}/{len(flattened)}")
            print(
                f"Non unique monthly indices: {len([r for r in flattened if r['error'] == 'Monthly index not unique'])}/{len(flattened)}")
            return flattened

        @task
        def validate_monthly_indices_per_tenant(data: Result) -> List[Result]:
            tenant = data["key"]
            indices = retrieve("fetch_and_group_indices", tenant)
            results = []

            for monthly_alias in expected_monthly_aliases(tenant):
                monthly_indices = [index for index in indices if monthly_alias in index]
                if not monthly_indices:
                    results.append(failure(key=tenant, value=monthly_alias, error="Missing index"))
                    continue
                if len(monthly_indices) != 1:
                    results.append(failure(key=tenant, value=monthly_alias, error=f"Monthly index not unique"))
                    continue
                index = monthly_indices[0]
                aliases = retrieve("fetch_and_group_aliases", index)
                if not all([any(r.fullmatch(alias) for r in ALIAS_REGEX_MAPPING.values()) for alias in aliases]):
                    results.append(failure(key=tenant, value=index, error="Alias not complete"))
                    continue

                results.append(success(key=tenant, value=index))
            return results

        expanded = validate_monthly_indices_per_tenant.expand(data=data)
        return flatten_results(results=expanded)


    @task_group
    def missing_monthly_indices(upstream: List[Result]):
        @task
        def extract_missing_monthly_indices(data: list[Result]):
            return [success(key=d["key"], value=d["value"]) for d in data if d["error"] == "Missing index"]

        @task
        def create_missing_monthly_indices(data: Result):
            return data

        extracted = extract_missing_monthly_indices(data=upstream)
        return create_missing_monthly_indices.expand(data=extracted)


    @task_group
    def indices_with_missing_aliases(upstream: Result):
        @task
        def extract_indices_with_missing_aliases(data: list[Result]):
            return [success(key=d["key"], value=d["value"]) for d in data if d["error"] == "Alias not complete"]

        @task
        def assign_missing_aliases_to_indices(data: Result):
            return data

        extracted = extract_indices_with_missing_aliases(data=upstream)
        return assign_missing_aliases_to_indices.expand(data=extracted)


    fga = fetch_and_group_aliases()
    fgi = fetch_and_group_indices()
    ilm = ilm_settings(fgi)
    fga.set_downstream(ilm)

    validated = validate_monthly_indices(ilm)

    indices_with_missing_aliases(validated)
    missing_monthly_indices(validated)


wip_dag()

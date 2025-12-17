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
    def ilm_settings(upstream: List[Result]):
        @task(task_id="extract")
        def validate(data):
            tenant = data["key"]
            indices = xcom_pull("fetch_and_group_indices.push", tenant)
            il_list = []
            for index in indices:
                print(f"Pulling settings for {index}")
                if s := xcom_pull("fetch_settings", index):
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

            rollover_aliases = set(il["rollover_alias"] for il in il_list)
            if not all(ALIAS_REGEX_MAPPING["rollover"].match(a) for a in rollover_aliases):
                return failure(key=tenant, error=f"Invalid rollover alias {rollover_aliases}")

            if len(rollover_aliases) != 1:
                return failure(key=tenant, error=f"Rollover alias is not unique {rollover_aliases}")

            result = {
                "retention": next(iter(set(POLICY_MAPPING.get(p) for p in policies))),
                "rollover_alias": next(iter(rollover_aliases)),
            }

            return success(key=tenant, value=result)

        e = validate.expand(data=upstream)
        return push(e)

    def is_valid_index_template(tenant, index_template):
        num_months = retrieve("ilm_settings", tenant)["retention"]
        return index_template == expected_index_template(tenant=tenant, retention_months=num_months)

    @task_group
    def index_templates(upstream: List[Result]):
        @task(task_id="extract")
        def validate(data):
            tenant = data["key"]
            index_template = xcom_pull("fetch_index_templates", f"{tenant}-rollover")
            _ = index_template.pop("composed_of")

            if not is_valid_index_template(tenant, index_template):
                return failure(key=tenant, error=f"Invalid template: {index_template}")
            return success(key=tenant, value=index_template)

        e = validate.expand(data=upstream)
        return push(e)

    def generate_past_month_starts(n):
        current_month_start = datetime.datetime.today().replace(day=1, hour=0, minute=0, second=0, tzinfo=datetime.timezone.utc) + relativedelta(months=1)
        return [current_month_start - relativedelta(months=i) for i in range(n)]


    @task_group
    def validate_monthly_indices_and_aliases(upstream: List[Result]):
        @task
        def flatten_results(results: List[List[Result]]) -> List[Result]:
            flattened = [item for sublist in results for item in sublist]
            print(f"""
                Flattened result size: {len(flattened)}
                OK monthly indices: {len([r for r in flattened if r['success']])}/{len(flattened)}
                Missing monthly indices: {len([r for r in flattened if r['error'] == 'Missing index'])}/{len(flattened)}
                Indices with missing aliases: {len([r for r in flattened if r['error'] == 'Alias not complete'])}/{len(flattened)}
                Non unique monthly indices: {len([r for r in flattened if r['error'] == 'Monthly index not unique'])}/{len(flattened)}
            """)
            return flattened

        @task
        def validate_monthly_indices_per_tenant(data: Result) -> List[Result]:
            tenant = data["key"]
            indices = retrieve("fetch_and_group_indices", tenant)
            num_months = retrieve("ilm_settings", tenant)["retention"]

            results = []
            for month_start in generate_past_month_starts(num_months):
                monthly_indices = [index for index in indices if f"{tenant}-{month_start:%Y-%m-%d}" in index]
                if not monthly_indices:
                    results.append(failure(key=tenant, value=month_start, error="Missing index"))
                    continue
                if len(monthly_indices) != 1:
                    results.append(failure(key=tenant, value=month_start, error=f"Monthly index not unique"))
                    continue
                index = monthly_indices[0]
                aliases = retrieve("fetch_aliases", index)
                if not all([any(r.fullmatch(alias) for r in ALIAS_REGEX_MAPPING.values()) for alias in aliases["aliases"].keys()]):
                    results.append(failure(key=tenant, value=index, error="Alias not complete"))
                    continue

                results.append(success(key=tenant, value=index))
            return results

        expanded = validate_monthly_indices_per_tenant.expand(data=upstream)
        return flatten_results(results=expanded)


    @task_group
    def add_missing_months(upstream: List[Result]):
        @task
        def extract_missing_monthly_indices(data: List[Result]):
            extracted = [success(key=d["key"], value=d["value"]) for d in data if d["error"] == "Missing index"]
            for r in extracted:
                print(f"Missing monthly index: {r['key']}-{r['value']}")
            return extracted

        @task
        def create_missing_monthly_indices(data: Result):
            # pkgdentest_topic-builder-pkgdentest.medallia.com-pkgdentest-2026-01-01
            # index details: tenant id, origination_date
            month = data['value']
            tenant = data["key"]
            tenant_id = 1234

            index = f"{tenant}-{month:%Y-%m-%d}-{tenant_id}-0"
            origination_date = data['value'].timestamp()
            payload = {
                "settings": {"index.lifecycle.origination_date": origination_date},
                "aliases": generate_aliases(index)
            }
            return data

        return create_missing_monthly_indices.expand(data=extract_missing_monthly_indices(data=upstream))


    @task_group
    def indices_with_missing_aliases(upstream: List[Result]):
        @task
        def extract_indices_with_missing_aliases(data: List[Result]):
            extracted = [success(key=d["key"], value=d["value"]) for d in data if d["error"] == "Alias not complete"]
            for r in extracted:
                print(f"Indices with missing aliases: {r['key']}/{r['value']}")
            return extracted

        @task
        def assign_missing_aliases_to_indices(data: Result):
            return data

        return assign_missing_aliases_to_indices.expand(data=extract_indices_with_missing_aliases(data=upstream))

    f1 = fetch_aliases(hook=hook_get)
    f2 = fetch_mappings(hook=hook_get)
    f3 = fetch_settings(hook=hook_get)
    f4 = fetch_index_templates(hook=hook_get)
    fgi = fetch_and_group_indices()
    [f1, f2, f3, f4] >> fgi



    validated = validate_monthly_indices_and_aliases(index_templates(upstream=ilm_settings(upstream=fgi)))

    indices_with_missing_aliases(validated)
    add_missing_months(validated)


wip_dag()

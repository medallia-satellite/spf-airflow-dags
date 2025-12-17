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

    @task_group
    def fetch(hook: HttpHook):
        def _filter_and_push(results, filter_fn) -> Result:
            for k, v in results.items():
                if filter_fn(k):
                    xcom_push(k, v)
            return success("all", list(results.keys()))

        @task
        def aliases(h: HttpHook) -> Result:
            results = fetch_from_endpoint(h, "/_aliases")
            return _filter_and_push(results, lambda x: INDEX_REGEX.match(x))

        @task
        def ilm_settings(h: HttpHook) -> Result:
            results = fetch_from_endpoint(h, "/_settings/index.lifecycle.name,index.lifecycle.rollover_alias")
            return _filter_and_push(results, lambda x: INDEX_REGEX.match(x))

        @task
        def mappings(h: HttpHook) -> Result:
            results = fetch_from_endpoint(h, "/_mappings")
            return _filter_and_push(results, lambda x: INDEX_REGEX.match(x))

        @task
        def index_template(h: HttpHook) -> Result:
            results = fetch_from_endpoint(h, "/_index_template/*-rollover")
            return _filter_and_push(
                {r["name"]: r["index_template"] for r in results["index_templates"]},
                lambda x: ALIAS_REGEX_MAPPING["rollover"].match(x),
            )

        @task
        def indices(h: HttpHook) -> List[str]:
            results = fetch_from_endpoint(h, "/_cat/indices?h=index&format=json")
            return [r["index"] for r in results if INDEX_REGEX.match(r["index"])]

        f1 = aliases(h=hook)
        f2 = mappings(h=hook)
        f3 = ilm_settings(h=hook)
        f4 = index_template(h=hook)
        f5 = indices(h=hook)
        [f1, f2, f3, f4] >> f5
        return f5

    @task_group
    def group_indices(upstream):
        @task
        def group_by_tenant(indices: list):
            result = defaultdict(list)
            for index in indices:
                result[BASE_REGEX.search(index).group(0)].append(index)
            return [success(key=k, value=v) for k, v in result.items()]

        grouped = group_by_tenant(upstream)
        return push(grouped)

    @task_group
    def validate(upstream):
        @task
        def ilm_settings(data):
            tenant = data["key"]
            indices = xcom_pull("group_indices.push", tenant)
            il_list = []
            for index in indices:
                if s := xcom_pull("fetch.ilm_settings", index):
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

            xcom_push(key=tenant, value={
                "retention": next(iter(set(POLICY_MAPPING.get(p) for p in policies))),
                "rollover_alias": next(iter(rollover_aliases)),
            })

            return success(key=tenant, value=None)

        @task
        @chain_on_success
        def index_templates(data):
            tenant = data["key"]
            settings = xcom_pull("validate.ilm_settings", tenant)[0]

            index_template = xcom_pull("fetch.index_template", settings["rollover_alias"])
            _ = index_template.pop("composed_of")
            if index_template != expected_index_template(tenant=tenant, retention_months=settings["retention"]):
                return failure(key=tenant, error=f"Invalid index template: {index_template}")
            xcom_push(key=tenant, value=index_template)
            return success(key=tenant, value=index_template)

        @task
        @chain_on_success
        def monthly_aliases(data):
            tenant = data["key"]
            indices = retrieve("group_indices", tenant)
            num_months = xcom_pull("validate.ilm_settings", tenant)[0]["retention"]

            active_indices = []
            for month_start in generate_past_month_starts(num_months):
                active_indices += [index for index in indices if f"{tenant}-{month_start:%Y-%m-%d}" in index]

            for index in active_indices:
                aliases = xcom_pull("fetch.aliases", index)
                if not all([any(r.fullmatch(alias) for r in ALIAS_REGEX_MAPPING.values()) for alias in aliases["aliases"].keys()]):
                    return failure(key=tenant, error="Alias not complete")

            return success(key=tenant, value=None)

        @task
        @chain_on_success
        def monthly_indices(data):
            tenant = data["key"]
            indices = retrieve("group_indices", tenant)
            num_months = xcom_pull("validate.ilm_settings", tenant)[0]["retention"]

            for month_start in generate_past_month_starts(num_months):
                if not [index for index in indices if f"{tenant}-{month_start:%Y-%m-%d}" in index]:
                    return failure(key=tenant, error="Missing index")
            return success(key=tenant, value=None)

        @task
        def categorize_errors(ilm_settings_results, index_templates_results, monthly_aliases_results, monthly_indices_results):
            errors = {
                "ilm_settings": [r["key"] for r in ilm_settings_results if not r["success"]],
                "index_templates": [r["key"] for r in index_templates_results if not r["success"] and "Invalid index template" in r["error"]],
                "monthly_aliases": [r["key"] for r in monthly_aliases_results if not r["success"] and "Alias" in r["error"]],
                "monthly_indices": [r["key"] for r in monthly_indices_results if not r["success"] and "Missing index" in r["error"]],
            }
            print(json.dumps(errors, indent=2))
            return errors

        t1 = ilm_settings.expand(data=upstream)
        t2 = index_templates.expand(data=t1)
        t3 = monthly_aliases.expand(data=t2)
        t4 = monthly_indices.expand(data=t2)
        return categorize_errors(t1, t2, t3, t4)

    def generate_past_month_starts(n):
        current_month_start = datetime.datetime.today().replace(day=1, hour=0, minute=0, second=0, tzinfo=datetime.timezone.utc) + relativedelta(months=1)
        return [current_month_start - relativedelta(months=i) for i in range(n)]

    @task_group
    def fix_monthly_aliases(upstream):
        @task
        def fix(data):
            print(data)
        return fix.expand(data=upstream["monthly_aliases"])

    @task_group
    def fix_monthly_indices(upstream):
        @task
        def fix(data):
            print(data)
        return fix.expand(data=upstream["monthly_indices"])

    @task_group
    def fix_index_templates(upstream):
        @task
        def fix(data):
            print(data)
        return fix.expand(data=upstream["index_templates"])

    @task_group
    def fix_ilm_settings(upstream):
        @task
        def fix(data):
            print(data)
        return fix.expand(data=upstream["ilm_settings"])

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
            indices = retrieve("group_indices", tenant)
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
                aliases = xcom_pull("fetch.aliases", index)
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

    hook_get = HttpHook(method='GET', http_conn_id='es-wordtags')

    f = fetch(hook=hook_get)
    fgi = group_indices(f)
    v = validate(fgi)
    fix_ilm_settings(v)
    fix_index_templates(v)
    fix_monthly_indices(v)
    fix_monthly_aliases(v)

    # validated = validate_monthly_indices_and_aliases(index_templates(upstream=ilm_settings(upstream=fgi)))
    #
    # indices_with_missing_aliases(validated)
    # add_missing_months(validated)


wip_dag()

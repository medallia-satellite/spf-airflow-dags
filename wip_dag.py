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

    def aaaaaa(index: str) -> bool:
        aliases = retrieve("fetch_and_group_aliases", index)
        return all([any(r.fullmatch(alias) for r in ALIAS_REGEX_MAPPING.values()) for alias in aliases])

    @task
    def check_monthly_indices(data: Result):
        tenant = data["key"]
        indices = retrieve("fetch_and_group_indices", tenant)
        result = []

        for monthly_alias in expected_monthly_aliases(tenant):
            monthly_indices = [index for index in indices if monthly_alias in index]
            if not monthly_indices:
                result.append(failure(key=tenant, error=f"Missing index: {monthly_alias}"))
                continue
            if len(monthly_indices) != 1:
                result.append(failure(key=tenant, error=f"Monthly index not unique: {monthly_indices}"))
                continue
            if not aaaaaa(monthly_indices[0]):
                result.append(failure(key=tenant, error="alias"))
                continue
            result.append(success(key=tenant, value=monthly_indices[0]))

        return result

    @task
    def assign_missing_aliases(data: Result):
        if not data["success"] and data["error"] != "alias":
            return data


        return data


    @task_group
    def reconcile(data: List[Result]):
        expanded = check_monthly_indices.expand(data=data)
        return push(flatten_results(expanded))

    fga = fetch_and_group_aliases()
    fgi = fetch_and_group_indices()
    ilm = ilm_settings(fgi)
    [fga, fgi] >> ilm
    rec = reconcile(ilm)
    ilm >> rec
    rep = report_errors(stages=["ilm_settings", "reconcile"])
    rec >> rep

wip_dag()

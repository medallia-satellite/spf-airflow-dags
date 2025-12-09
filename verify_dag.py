from collections import defaultdict

from airflow.decorators import dag, task_group
from airflow.providers.http.hooks.http import HttpHook

from repo.elasticsearch_client import *
from repo.fix_and_verify import *
from repo.utils import *


@dag(
    dag_display_name="Verify Monthly Indices",
    tags=["spf", "test", "poc"],
    description="This DAG verifies monthly indices.",
    catchup=False,
)
def verify_monthly_indices_dag():
    hook_get = HttpHook(method='GET', http_conn_id='es-wordtags')

    @task(task_id="extract")
    @chain_on_success
    def extract_mapping(data):
        _mappings = data["value"]
        sample = next(iter(_mappings))
        if not all(m == sample for m in _mappings):
            return failure(key=data["key"], error=f"different mappings {_mappings}")

        if sample != default_index_mappings():
            return failure(key=data["key"], error=f"invalid mapping {sample}")

        return success(key=data["key"], value=sample)

    @task(task_id="extract")
    @chain_on_success
    def extract_ilm_setting(data):
        instance = data["key"]
        il_list = [s["settings"]["index"]["lifecycle"] for s in data["value"]]

        if any("name" not in il for il in il_list):
            return failure(key=instance, error=f"No lifecycle policy {il_list=}")

        policies = set(il["name"] for il in il_list)
        if not all(p in POLICY_MAPPING for p in policies):
            return failure(key=instance, error=f"Invalid policies: {policies=}")

        if len(set(POLICY_MAPPING.get(p) for p in policies)) != 1:
            return failure(key=instance, error=f"Retention period is not unique: {policies=}")

        if any("rollover_alias" not in il for il in il_list):
            return failure(key=instance, error=f"No rollover alias {[il for il in il_list if 'rollover_alias' not in il]}")

        rollover_aliases = set(il["rollover_alias"] for il in il_list)
        if not all(ALIAS_REGEX_MAPPING["rollover"].match(a) for a in rollover_aliases):
            return failure(key=instance, error=f"Invalid rollover alias {rollover_aliases=}")

        if len(rollover_aliases) != 1:
            return failure(key=instance, error="Invalid rollover alias {rollover_aliases=}")

        result = {
            "retention": next(iter(set(POLICY_MAPPING.get(p) for p in policies))),
            "rollover_alias": next(iter(rollover_aliases)),
        }

        return success(key=instance, value=result)


    @task_group
    def fetch_aliases_and_indices_by_tenant():
        @task
        def group_by_tenant(aliases: list):
            result = defaultdict(list)
            for alias_entry in aliases:
                alias = alias_entry["alias"]
                if not any(r.fullmatch(alias) for r in ALIAS_REGEX_MAPPING.values()):
                    continue
                result[BASE_REGEX.match(alias).group(0)].append(alias_entry)
            return [success(key=k, value=v) for k, v in result.items()]

        fetched = fetch_aliases(hook=hook_get)
        grouped = group_by_tenant(fetched)
        return push(grouped)

    @task
    def group_aliases_by_index(data):
        result = defaultdict(list)
        for alias_entry in data["value"]:
            result[alias_entry["index"]].append(alias_entry["alias"])
        return result

    @task
    def check_indices_with_3_aliases(data):
        mapping = defaultdict(list)
        for alias_entry in data["value"]:
            mapping[alias_entry["index"]].append(alias_entry["alias"])

        if any(len(aliases) < 3 for aliases in mapping.values()):
            return failure(key=data["key"], error=f"Too few aliases: {[(index, aliases) for index, aliases in mapping.items() if len(aliases) < 3]}")
        else:
            return success(key=data["key"], value="")

    @task_group
    def missing_aliases(data):
        grouped = group_aliases_by_index(data=data)
        r = check_indices_with_3_aliases.expand(data=grouped)
        print_errors(r)
        return push(filter_errors(r))

    @task_group
    def lifecycle_settings(data):
        f = fetch_alias_settings.partial(hook=hook_get).expand(data=data)
        e = extract_ilm_setting.expand(data=f)
        print_errors(e)
        return push(filter_errors(e))

    @task_group
    def mappings(data):
        f = fetch_alias_mappings.partial(hook=hook_get).expand(data=data)
        e = extract_mapping.expand(data=f)
        print_errors(e)
        return push(filter_errors(e))


    @task
    def identify_missing_months(data):
        instance = data["key"]
        aliases = [alias['alias'] for alias in retrieve("fetch_aliases_and_indices_by_tenant", instance)]
        num_months = retrieve("lifecycle_settings", instance)["retention"]

        today = datetime.date.today()
        start_date = today.replace(day=1) + relativedelta(months=1)

        missing = []
        for month, monthly_alias in monthly_aliases(instance, start_date, num_months):
            if monthly_alias not in aliases:
                missing.append(month)

        return success(key=instance, value=missing)

    @task_group
    def missing_months(data):
        m = identify_missing_months.expand(data=data)
        filtered = filter_empty(m)
        return push(filter_errors(filtered))


    return missing_months(lifecycle_settings(data=mappings(data=missing_aliases(data=fetch_aliases_and_indices_by_tenant()))))

verify_monthly_indices_dag()

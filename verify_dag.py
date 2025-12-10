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

        if sample["mappings"] != default_index_mappings():
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
    def fetch_data():
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
    def group_aliases_by_index(data: Result):
        instance = data["key"]
        result = defaultdict(list)
        for alias_entry in retrieve("fetch_data", instance):
            result[alias_entry["index"]].append(alias_entry["alias"])
        return success(key=instance, value=dict(result))

    @task
    def check_indices_with_3_aliases(data):
        if any(len(aliases) < 3 for aliases in data["value"].values()):
            result = {i: a for i,a in data["value"].items() if len(a) < 3}
            return failure(key=data["key"], error=f"Too few aliases: {result=}")
        else:
            return success(key=data["key"], value="")

    @task_group
    def aliases(data: List[Result]):
        grouped = group_aliases_by_index.expand(data=data)
        r = check_indices_with_3_aliases.expand(data=grouped)
        return push(filter_errors(r))

    @task_group
    def ilm_settings(data: List[Result]):
        f = fetch_alias_settings.partial(hook=hook_get).expand(data=data)
        e = extract_ilm_setting.expand(data=f)
        return push(filter_errors(e))

    @task_group
    def mappings(data: List[Result]):
        f = fetch_alias_mappings.partial(hook=hook_get).expand(data=data)
        e = extract_mapping.expand(data=f)
        return push(filter_errors(e))

    @task
    def expected_monthly_aliases(data: Result):
        instance = data["key"]
        num_months = retrieve("ilm_settings", data["key"])["retention"]
        today = datetime.date.today()
        start_date = today.replace(day=1) + relativedelta(months=1)
        result = [
            f"{instance}-{(start_date - relativedelta(months=i)):%Y-%m-%d}"
            for i in range(num_months)
        ]
        return success(key=instance, value=result)



    @task_group
    def monthly_aliases(data: List[Result]):
        @task
        def aliases_in_retention(r: Result):
            instance = r["key"]
            instance_aliases = [alias['alias'] for alias in retrieve("fetch_data", instance)]

            if missing := [alias for alias in r["value"] if alias not in instance_aliases]:
                return failure(key=instance, error=f"Missing aliases {missing=}")
            return success(key=instance, value="")

        expected = expected_monthly_aliases.expand(data=data)
        filtered = filter_errors(aliases_in_retention.expand(data=expected))
        return push(filter_errors(filtered))


    @task_group
    def expired_aliases(data: List[Result]):
        @task
        def filter_expired(r: Result):
            instance = r["key"]
            instance_aliases = [alias['alias'] for alias in retrieve("fetch_data", instance)]
            if expired := [alias for alias in instance_aliases if alias not in r["value"]]:
                return failure(key=instance, error=f"Expired aliases {expired=}")
            return success(key=instance, value="")

        expected = expected_monthly_aliases.expand(data=data)
        filtered = filter_errors(filter_expired.expand(data=expected))

        return push(filter_errors(filtered))

    fetch_data_tg = fetch_data()
    aliases_tg = aliases(fetch_data_tg)
    mappings_tg = mappings(fetch_data_tg)
    lifecycle_settings_tg = ilm_settings(fetch_data_tg)
    monthly_aliases_tg = monthly_aliases(lifecycle_settings_tg)
    expired_aliases_tg = expired_aliases(lifecycle_settings_tg)

    report_errors_tg = report_errors(stages=["aliases", "mappings", "ilm_settings", "monthly_aliases"])
    [aliases_tg, mappings_tg, monthly_aliases_tg, expired_aliases_tg] >> report_errors_tg
    return report_errors_tg

verify_monthly_indices_dag()

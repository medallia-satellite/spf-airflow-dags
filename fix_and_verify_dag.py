from collections import defaultdict

from airflow.decorators import dag, task_group
from airflow.providers.http.hooks.http import HttpHook

from repo.elasticsearch_client import *
from repo.fix_and_verify import *
from repo.utils import *


@dag(
    dag_display_name="FNV",
    tags=["spf", "test", "poc"],
    description="This is a POC",
    catchup=False,
)
def fnv():
    hook_get = HttpHook(method='GET', http_conn_id='es-wordtags')
    hook_put = HttpHook(method='PUT', http_conn_id='es-wordtags')
    hook_post = HttpHook(method='POST', http_conn_id='es-wordtags')


    @task(task_id="extract")
    @chain_on_success
    def extract_mapping(data):
        _mappings = data["value"]
        sample = next(iter(_mappings))
        if not all(m == sample for m in _mappings):
            return failure(key=data["key"], error=f"different mappings {_mappings}")

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
    def add_missing_aliases():
        @task
        def group_aliases_by_index(aliases):
            result = defaultdict(list)
            for alias_entry in aliases:
                index = alias_entry["index"]
                if not INDEX_REGEX.fullmatch(index):
                    continue
                result[index].append(alias_entry["alias"])
            return result

        @task
        def indices_with_missing_aliases(indices, aliases):
            result = []
            for index in indices:
                if index not in aliases:
                    print(f"{index} - no alias")
                    result.append(success(key=index, value=None))
                elif len(aliases[index]) < 3:
                    print(f"{index} - {aliases[index]}")
                    result.append(success(key=index, value=None))
            return result

        a = fetch_aliases(hook=hook_get)
        i = fetch_indices(hook=hook_get)
        m = indices_with_missing_aliases(i, group_aliases_by_index(a))
        return push(add_aliases.partial(hook=hook_post).expand(data=m))

    @task_group
    def reconcile_aliases():
        @task(task_id="aliases_by_instance")
        def group_aliases_by_instance(aliases: list):
            grouped = defaultdict(list)
            for alias_entry in aliases:
                alias = alias_entry["alias"]
                if not any(r.fullmatch(alias) for r in ALIAS_REGEX_MAPPING.values()):
                    continue
                grouped[BASE_REGEX.match(alias).group(0)].append(alias_entry)
            return [success(key=k, value=v) for k, v in grouped.items()]

        fetched_aliases = fetch_aliases(hook=hook_get)
        return push(group_aliases_by_instance(fetched_aliases))

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
    def aaaaaaaaa(data):
        instance = "aa_topic-builder-aa.medallia.com-aa"
        tenant_id = "101485"
        date = "2024-06-01"
        suffix = "000004"
        result = {
            "index_name": f"%3Cseaas-{instance}-%7B{date}%7Byyyy-MM-dd%7D%7D-{tenant_id}-{suffix}%3E"
        }
        return success(key=data["key"], value=result)

    @task
    @chain_on_success
    def extract_instance_details(data):
        instance = data["key"]
        indices = [a["index"] for a in data["value"]]
        if not any(INDEX_REGEX.fullmatch(i) for i in indices):
            return failure(key=instance, error=f"Invalid indices {indices=}")

        t = set(tenant_id_from_index(i) for i in indices)
        if len(t) != 1:
            return failure(key=instance, error=f"Multiple tenant_ids found {indices=}")

        aliases = retrieve("reconcile_aliases", instance)
        num_months = retrieve("lifecycle_settings", instance)["retention"]
        indices = [a["index"] for a in aliases]

        return success(key=instance, value="")

    @task
    def identify_missing_months(data):
        instance = data["key"]
        aliases = retrieve("reconcile_aliases", instance)
        num_months = retrieve("lifecycle_settings", instance)["retention"]

        write_aliases = {alias['alias']: alias["index"] for alias in aliases if
                ALIAS_REGEX_MAPPING["write"].fullmatch(alias['alias']) and alias["is_write_index"]}

        today = datetime.date.today()
        start_date = today.replace(day=1) + relativedelta(months=1)

        missing_aliases = []
        for monthly_alias in monthly_aliases(instance, start_date, num_months):
            if monthly_alias not in write_aliases:
                missing_aliases.append(monthly_alias)

        return success(key=instance, value=missing_aliases)

    @task_group
    def add_missing_months(data):
        missing_months = identify_missing_months.expand(data=data)
        filtered = filter_empty(missing_months)
        processed = aaaaaaaaa.expand(data=filtered)
        return push(filter_errors(processed))

    mm = add_missing_aliases()
    p = add_missing_months(lifecycle_settings(data=mappings(data=reconcile_aliases())))

    mm >> p

fnv()

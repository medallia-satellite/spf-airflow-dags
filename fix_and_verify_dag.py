import datetime
import json
from collections import defaultdict

from airflow.decorators import task, dag, task_group
from airflow.providers.http.hooks.http import HttpHook
from dateutil.relativedelta import relativedelta

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

    @task
    def fetch_aliases():
        response = hook_get.run(
            endpoint='/_cat/aliases?h=alias,index,is_write_index',
            headers={'Accept': 'application/json'},
        )
        hook_get.check_response(response)
        return [r for r in response.json() if BASE_REGEX.match(r["alias"])]

    @task
    def fetch_indices():
        response = hook_get.run(
            endpoint='/_cat/indices?h=index&format=json',
            headers={'Accept': 'application/json'},
        )
        hook_get.check_response(response)
        return [r["index"] for r in response.json() if INDEX_REGEX.match(r["index"])]

    @task(task_id="fetch")
    def fetch_alias_settings(alias):
        alias = alias["instance"]
        response = hook_get.run(
            endpoint=f'/{alias}/_settings/'
                     f'index.lifecycle.name,'
                     f'index.lifecycle.rollover_alias',
            headers={'Accept': 'application/json'},
        )
        hook_get.check_response(response)
        return success(instance=alias, value=list(response.json().values()))

    @task(task_id="fetch")
    def fetch_alias_mappings(alias):
        alias = alias["instance"]
        response = hook_get.run(
            endpoint=f'/{alias}/_mapping',
            headers={'Accept': 'application/json'},
        )
        hook_get.check_response(response)
        return success(instance=alias, value=list(response.json().values()))

    @task
    def create_index(index_name):
        response = hook_put.run(
            endpoint=f'/{index_name}',
            headers={'Accept': 'application/json'},
            data=json.dumps(INDEX_SETTINGS_AND_MAPPINGS)
        )
        hook_get.check_response(response)
        return response.json()

    @task(task_id="extract")
    def extract_mapping(input_data):
        if not input_data["success"]:
            return input_data

        mappings = input_data["value"]
        sample = next(iter(mappings))
        if not all(m == sample for m in mappings):
            return failure(instance=input_data["instance"], error=f"different mappings {mappings}")

        return success(instance=input_data["instance"], value=sample)

    @task(task_id="extract")
    def extract_ilm_setting(input_data):
        if not input_data["success"]:
            return input_data
        instance = input_data["instance"]
        il_list = [s["settings"]["index"]["lifecycle"] for s in input_data["value"]]

        if any("name" not in il for il in il_list):
            return failure(instance=instance, error=f"No lifecycle policy {il_list=}")

        policies = set(il["name"] for il in il_list)
        if not all(p in POLICY_MAPPING for p in policies):
            return failure(instance=instance, error=f"Invalid policies: {policies=}")

        if len(set(POLICY_MAPPING.get(p) for p in policies)) != 1:
            return failure(instance=instance, error=f"Retention period is not unique: {policies=}")

        if any("rollover_alias" not in il for il in il_list):
            return failure(instance=instance, error=f"No rollover alias {[il for il in il_list if 'rollover_alias' not in il]}")

        rollover_aliases = set(il["rollover_alias"] for il in il_list)
        if not all(ALIAS_REGEX_MAPPING["rollover"].match(a) for a in rollover_aliases):
            return failure(instance=instance, error=f"Invalid rollover alias {rollover_aliases=}")

        if len(rollover_aliases) != 1:
            return failure(instance=instance, error="Invalid rollover alias {rollover_aliases=}")

        result = {
            "retention": next(iter(set(POLICY_MAPPING.get(p) for p in policies))),
            "rollover_alias": next(iter(rollover_aliases)),
        }


        return success(instance=instance, value=result)

    @task
    def assert_all_indices_have_read_alias(all_indices, all_aliases):
        indices_with_read_alias = [r["index"] for r in all_aliases if ALIAS_REGEX_MAPPING["read"].match(r["alias"])]
        indices_without_read_alias = [index for index in all_indices if index not in indices_with_read_alias]
        assert len(indices_without_read_alias) == 0, f"Indices without read alias: {indices_without_read_alias=}"

    @task(task_id="aliases_by_instance")
    def group_aliases_by_instance(all_aliases: list):
        grouped = defaultdict(list)
        for alias_entry in all_aliases:
            alias = alias_entry["alias"]
            if not any(r.fullmatch(alias) for r in ALIAS_REGEX_MAPPING.values()):
                continue
            grouped[BASE_REGEX.match(alias).group(0)].append(alias_entry)
        return [success(instance=k, value=v) for k, v in grouped.items()]

    @task
    def identify_missing_months(input_data):
        regex = ALIAS_REGEX_MAPPING["write"]

        instance = input_data["instance"]
        aliases = retrieve("fetch_all_aliases", instance)
        num_months = retrieve("lifecycle_settings", instance)["retention"]

        write_aliases = {alias['alias']: alias["index"] for alias in aliases if
                regex.fullmatch(alias['alias']) and alias["is_write_index"]}

        today = datetime.date.today()
        start_date = today.replace(day=1) + relativedelta(months=1)

        missing_aliases = []
        for monthly_alias in monthly_aliases(instance, start_date, num_months):
            if monthly_alias not in write_aliases:
                missing_aliases.append(monthly_alias)

        return success(instance=instance, value=missing_aliases)

    @task
    def aaaaaaaaa(input_data):
        instance = "aa_topic-builder-aa.medallia.com-aa"
        tenant_id = "101485"
        date = "2024-06-01"
        suffix = "000004"
        result = {
            "index_name": f"%3Cseaas-{instance}-%7B{date}%7Byyyy-MM-dd%7D%7D-{tenant_id}-{suffix}%3E"
        }
        return success(instance=input_data["instance"], value=result)


    @task_group
    def fetch_all_aliases():
        fetched_aliases = fetch_aliases()
        fetched_indices = fetch_indices()
        assert_all_indices_have_read_alias(fetched_indices, fetched_aliases)
        return push(group_aliases_by_instance(fetched_aliases))

    @task_group
    def lifecycle_settings(input_data):
        extracted = extract_ilm_setting.expand(
            input_data=fetch_alias_settings.expand(alias=input_data)
        )
        print_errors(extracted)
        return push(filter_errors(extracted))


    @task_group
    def validate_mappings(input_data):
        extracted = extract_mapping.expand(
            input_data=fetch_alias_mappings.expand(alias=input_data)
        )
        print_errors(extracted)
        return push(filter_errors(extracted))

    @task_group
    def prepare_missing_months(input_data):
        missing_months = identify_missing_months.expand(input_data=input_data)
        filtered = filter_empty(missing_months)
        processed = aaaaaaaaa.expand(input_data=filtered)
        return push(filter_errors(processed))


    prepare_missing_months(lifecycle_settings(input_data=validate_mappings(input_data=fetch_all_aliases())))

fnv()

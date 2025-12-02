import datetime
import re
from collections import defaultdict, namedtuple
from dataclasses import dataclass
from pprint import pprint
from typing import Iterator, Union, Any, TypedDict, Dict, Optional

from airflow.decorators import task, dag, task_group
from airflow.providers.http.hooks.http import HttpHook
from click import get_current_context
from dateutil.relativedelta import relativedelta

BASE_PATTERN = r"(\w+)_topic-builder(-\w+)+(\.\w{2,4}){0,2}(\.\w+)(\.\w{2,4}){1,2}-\1"
BASE_REGEX = re.compile(BASE_PATTERN)

REGEX_MAPPING = {
    "read": re.compile(rf"^{BASE_PATTERN}$"),
    "write": re.compile(rf"^{BASE_PATTERN}" + r"-[0-9]{4}-[0-9]{2}-[0-9]{2}$"),
    "rollover": re.compile(rf"^{BASE_PATTERN}-rollover$"),
}

POLICY_MAPPING = {
	"M6": 6,
	"M6_rollover": 6,
	"M18": 18,
	"M18_rollover": 18,
	"M36": 36,
	"M36_rollover": 36,
}

class Result(TypedDict):
    success: bool
    instance: str
    error: Optional[str]
    value: Optional[Any]

def success(instance: str, value: Any) -> Result:
    return Result(success=True, instance=instance, error=None, value=value)

def failure(instance: str, error: str) -> Result:
    return Result(success=False, instance=instance, error=error, value=None)


def monthly_aliases(alias: str, start_date: datetime.date, num_months: int) -> Iterator[str]:
    current_date = start_date
    for _ in range(num_months):
        yield f"{alias}-{current_date:%Y-%m-%d}"
        current_date -= relativedelta(months=1)

@dag(
    dag_display_name="FNV",
    tags=["spf", "test", "poc"],
    description="This is a POC",
    catchup=False,
)
def fnv():
    hook_get = HttpHook(method='GET', http_conn_id='es-wordtags')

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
        return [r["index"] for r in response.json() if BASE_REGEX.search(r["index"])]

    @task
    def fetch_alias_settings(alias):
        response = hook_get.run(
            endpoint=f'/{alias}/_settings/'
                     f'index.lifecycle.name,'
                     f'index.lifecycle.rollover_alias',
            headers={'Accept': 'application/json'},
        )
        hook_get.check_response(response)
        return success(instance=alias, value=response.json())

    @task
    def fetch_alias_mappings(alias):
        response = hook_get.run(
            endpoint=f'/{alias}/_mapping',
            headers={'Accept': 'application/json'},
        )
        hook_get.check_response(response)
        return success(instance=alias, value=response.json())

    @task
    def extract_mapping(mappings):
        if not mappings["success"]:
            return mappings

        _mappings = mappings["value"].values()
        sample = next(iter(_mappings))
        if not all(m == sample for m in _mappings):
            return failure(instance=mappings["instance"], error=f"different mappings {_mappings}")
        return success(instance=mappings["instance"], value=sample)

    @task
    def extract_ilm_setting(settings):

        if not settings["success"]:
            return settings
        il_list = [s["settings"]["index"]["lifecycle"] for s in settings["value"].values()]

        if any("name" not in il for il in il_list):
            return failure(instance=settings["instance"], error=f"No lifecycle policy {il_list=}")

        policies = set(il["name"] for il in il_list)
        if not all(p in POLICY_MAPPING for p in policies):
            return failure(instance=settings["instance"], error=f"Invalid policies: {policies=}")

        if len(set(POLICY_MAPPING.get(p) for p in policies)) != 1:
            return failure(instance=settings["instance"], error=f"Retention period is not unique: {policies=}")

        if any("rollover_alias" not in il for il in il_list):
            return failure(instance=settings["instance"], error=f"No rollover alias {[il for il in il_list if 'rollover_alias' not in il]}")

        rollover_aliases = set(il["rollover_alias"] for il in il_list)
        if not all(REGEX_MAPPING["rollover"].match(a) for a in rollover_aliases):
            return failure(instance=settings["instance"], error=f"Invalid rollover alias {rollover_aliases=}")

        if len(rollover_aliases) != 1:
            return failure(instance=settings["instance"], error="Invalid rollover alias {rollover_aliases=}")

        return success(instance=settings["instance"], value={
            "retention": next(iter(set(POLICY_MAPPING.get(p) for p in policies))),
            "rollover_alias": next(iter(rollover_aliases)),
        })


    @task
    def assert_all_indices_have_read_alias(all_indices, all_aliases):
        indices_with_read_alias = [r["index"] for r in all_aliases if REGEX_MAPPING["read"].match(r["alias"])]
        indices_without_read_alias = [index for index in all_indices if index not in indices_with_read_alias]
        assert len(indices_without_read_alias) == 0, f"Indices without read alias: {indices_without_read_alias=}"


    @task
    def group_aliases_by_instance(all_aliases: list) -> list:
        grouped = defaultdict(list)
        for alias_entry in all_aliases:
            alias = alias_entry["alias"]
            if not any(r.fullmatch(alias) for r in REGEX_MAPPING.values()):
                continue
            grouped[BASE_REGEX.match(alias).group(0)].append(alias_entry)
        return [{"instance": k, "aliases": v} for k, v in grouped.items()]


    def retrieve_aliases(instance):
        context = get_current_context()
        ti = context["ti"]
        aaa = ti.xcom_pull(task_ids="group_aliases_by_instance")
        return [e for e in aaa if e["instance"] == instance]

    @task
    def identify_missing_write_aliases(input_data):
        instance = input_data["instance"]
        aliases = retrieve_aliases(instance)

        num_months = input_data["value"]["retention"]
        regex = REGEX_MAPPING["write"]

        write_aliases= {alias['alias']: alias["index"] for alias in aliases if
                regex.fullmatch(alias['alias']) and alias["is_write_index"]}

        today = datetime.date.today()
        start_date = today.replace(day=1) + relativedelta(months=1)

        missing_aliases = []
        for monthly_alias in monthly_aliases(instance, start_date, num_months):
            if monthly_alias not in write_aliases:
                missing_aliases.append(monthly_alias)

        return success(instance=instance, value=missing_aliases)

    # @task(task_id="task_a")
    # def task_a(missing_aliases):
    #     for alias in missing_aliases:
    #         print(f"some_work on {alias}")
    #
    #
    # @task(task_id="task_b")
    # def task_b():
    #     print(f"OKAAA")
    #
    # @task.branch
    # def choose_branch(missing_aliases):
    #     if missing_aliases["success"] and len(list(missing_aliases["value"])) > 0:
    #         return 'aaaaaaaa.task_a'
    #     return 'aaaaaaaa.task_b'


    @task
    def collector(results):
        for r in results:
            print(f"Collected: {r}")
        return {"success": [r for r in results if r["success"]], "failure": [r for r in results if not r["success"]]}


    @task
    def collector2(results):
        return [r for r in results if r["success"]]



    @task_group
    def validate_lifecycle_settings(instance):
        return extract_ilm_setting(settings=fetch_alias_settings(alias=instance))

    @task_group
    def validate_mappings(instance):
        return extract_mapping(mappings=fetch_alias_mappings(alias=instance))

    fetched_aliases = fetch_aliases()
    fetched_indices = fetch_indices()

    t_read_alias = assert_all_indices_have_read_alias(fetched_indices, fetched_aliases)

    grouped = group_aliases_by_instance(fetched_aliases)
    a = collector2(validate_lifecycle_settings.partial().expand_kwargs(grouped))
    collector(validate_mappings.partial().expand_kwargs(grouped))
    identify_missing_write_aliases.partial().expand(a)

fnv()

#aa = aaaaaaaa.partial().expand_kwargs(group_aliases_by_instance(fetched_aliases))

# aaa = identify_missing_write_aliases(
#     instance=instance,
#     aliases=aliases,
#     ilm_setting=ilm_setting
# )

# branch = choose_branch(aaa)
# branch >> task_a(aaa)
# branch >> task_b()
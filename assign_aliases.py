import datetime
import re
from collections import defaultdict
from pprint import pprint
from typing import Iterator

from airflow.decorators import task, dag, task_group
from airflow.providers.http.hooks.http import HttpHook
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

def monthly_aliases(alias: str, start_date: datetime.date, num_months: int) -> Iterator[str]:
    current_date = start_date
    for _ in range(num_months):
        yield f"{alias}-{current_date:%Y-%m-%d}"
        current_date -= relativedelta(months=1)

@dag(
    dag_display_name="AAAAAAAAA",
    tags=["spf", "test", "poc"],
    description="This is a POC",
    catchup=False,
)
def aaaaaaaa():
    hook_get = HttpHook(method='GET', http_conn_id='es-wordtags')

    @task
    def fetch_indices():
        response = hook_get.run(
            endpoint='/_cat/indices?h=index&format=json',
            headers={'Accept': 'application/json'},
        )
        hook_get.check_response(response)
        print(response.json())
        return [r["index"] for r in response.json() if BASE_REGEX.search(r["index"])]

    @task
    def fetch_indices_in_alias(alias: list) -> list:
        response = hook_get.run(
            endpoint=f'/{alias}/_alias',
            headers={'Accept': 'application/json'},
        )
        hook_get.check_response(response)
        return response.json()

    @task
    def group_indices_by_base(indices: list) -> list:
        grouped = defaultdict(list)
        for index in indices:
            grouped[BASE_REGEX.search(index).group(0)].append(index)
        return [{"base_alias": k, "indices": v} for k, v in grouped.items()]

    @task
    def aaaaaaa(iii, aaa, base_alias):

        assert len(iii) == len(aaa), "different len"
        for i in aaa:
            assert i in iii, f"{i} not in {iii}"
            assert iii[i]["aliases"][base_alias]["is_write_index"] is False



    @task_group
    def verify_alias(base_alias, indices):
        indices_in_read_alias = fetch_indices_in_alias(alias=base_alias)
        aaaaaaa(iii=indices_in_read_alias, aaa=indices, base_alias=base_alias)

    verify_alias.partial().expand_kwargs(group_indices_by_base(fetch_indices()))


aaaaaaaa()
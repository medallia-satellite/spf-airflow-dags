import datetime
import re
from collections import defaultdict
from pprint import pprint
from typing import Iterator

from airflow.decorators import task, dag, task_group
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from dateutil.relativedelta import relativedelta

base_pattern = r"(\w+)_topic-builder(-\w+)+(\.\w{2,4}){0,2}(\.\w+)(\.\w{2,4}){1,2}-\1"
base_regex = re.compile(base_pattern)

regex_mapping = {
    "read": re.compile(rf"^{base_pattern}$"),
    "write": re.compile(rf"^{base_pattern}" + r"-[0-9]{4}-[0-9]{2}-[0-9]{2}$"),
    "rollover": re.compile(rf"^{base_pattern}-rollover$"),
}

policy_mapping = {
	"M6": 217.0,
	"M6_rollover": 217.0,
	"M18": 589.0,
	"M18_rollover": 589.0,
	"M36": 1147.0,
	"M36_rollover": 1147.0,
}

def monthly_aliases(alias: str, start_date: datetime.date, num_months: int) -> Iterator[str]:
    current_date = start_date
    for _ in range(num_months):
        yield f"{alias}-{current_date:%Y-%m-%d}"
        current_date -= relativedelta(months=1)

@dag(
    dag_display_name="SPF ES POC",
    tags=["spf", "test", "poc"],
    description="This is a POC",
    catchup=False,
)
def es_poc_dag():
    hook = HttpHook(method='GET', http_conn_id='es-wordtags')
    def filter_response(response):
        limited_response = [r for r in response if base_regex.match(r["alias"])]
        return limited_response[:10]

    fetch_data = SimpleHttpOperator(
        task_id='fetch_data',
        http_conn_id='es-wordtags',  # Refers to the connection ID defined in Airflow
        method='GET',
        endpoint='/_cat/aliases?h=alias,index,is_write_index',
        headers={'Accept': 'application/json'},
        response_check=lambda r: r.status_code == 200,
        response_filter=lambda r: filter_response(r.json()),
        log_response=False,
    )

    @task()
    def group_by_base_alias(input_data: list) -> list:
        filtered = defaultdict(list)
        for alias_entry in input_data:
            alias = alias_entry["alias"]
            if not any(r.fullmatch(alias) for r in regex_mapping.values()):
                continue
            filtered[base_regex.match(alias).group(0)].append(alias_entry)
        return [{"base_alias": k, "aliases": v} for k, v in filtered.items()]

    @task_group
    def verify_alias(base_alias, aliases):

        @task
        def fetch_policies(alias):
            response = hook.run(
                endpoint=f'/{alias}/_settings/index.lifecycle.name',
                headers={'Accept': 'application/json'},
            )
            hook.check_response(response)
            return set(p["settings"]["index"]["lifecycle"]["name"] for _, p in response.json().items())

        @task
        def validate_policies(policies):
            assert all(p in policy_mapping for p in policies), f"Invalid policies: {policies}"
            assert len(set(policy_mapping.get(p) for p in policies)) == 1, f"Retention period is not unique: {policies}"
            return policies

        @task
        def retention_from_policies(policies):
            return next(iter(set(policy_mapping.get(p) for p in policies)))

        @task
        def write_aliases(alias_list):
            regex = regex_mapping["write"]
            return {alias['alias']: alias["index"] for alias in alias_list if regex.fullmatch(alias['alias']) and alias["is_write_index"]}

        @task
        def verify_write_aliases(alias, alias_list, num_months):
            start_date = datetime.date.replace(
                datetime.datetime.today(), day=1
            ) + relativedelta(months=1)

            for monthly_alias in monthly_aliases(alias, start_date, num_months):
                assert monthly_alias in alias_list, f"Missing alias '{monthly_alias}'"

        @task()
        def group_aliases(aaa) -> dict:
            parsed = {t: [] for t in regex_mapping.keys()}
            for alias_entry in aaa:
                for t, regex in regex_mapping.items():
                    if regex.fullmatch(alias_entry["alias"]):
                        parsed[t].append(
                            {alias_entry['alias'], alias_entry['index'], alias_entry['is_write_index']})
                        break
            return parsed

        retention = retention_from_policies(policies=validate_policies(policies=fetch_policies(alias=base_alias)))

        verify_write_aliases(alias=base_alias, alias_list=write_aliases(alias=base_alias), num_months=retention)

        return group_aliases(aliases)

    grouped = group_by_base_alias(fetch_data.output)

    verify_alias.partial().expand(
        base_alias=grouped.map(lambda x: x["base_alias"]),
        aliases=grouped.map(lambda x: x["aliases"]),
    )


es_poc_dag()
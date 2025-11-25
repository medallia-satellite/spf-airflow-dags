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
    dag_display_name="SPF ES POC",
    tags=["spf", "test", "poc"],
    description="This is a POC",
    catchup=False,
)
def es_poc_dag():
    hook = HttpHook(method='GET', http_conn_id='es-wordtags')

    @task()
    def fetch_aliases():
        response = hook.run(
            endpoint='/_cat/aliases?h=alias,index,is_write_index',
            headers={'Accept': 'application/json'},
        )
        hook.check_response(response)
        return [r for r in response.json() if base_regex.match(r["alias"])]

    @task
    def fetch_policies(alias):
        response = hook.run(
            endpoint=f'/{alias}/_settings/index.lifecycle.name',
            headers={'Accept': 'application/json'},
        )
        hook.check_response(response)
        return set(p["settings"]["index"]["lifecycle"]["name"] for _, p in response.json().items())


    @task()
    def group_aliases_by_base(aliases: list) -> list:
        grouped = defaultdict(list)
        for alias_entry in aliases:
            alias = alias_entry["alias"]
            if not any(r.fullmatch(alias) for r in regex_mapping.values()):
                continue
            grouped[base_regex.match(alias).group(0)].append(alias_entry)
        return [{"base_alias": k, "aliases": v} for k, v in grouped.items()]


    @task
    def retention_from_policies(policies):
        assert all(p in policy_mapping for p in policies), f"Invalid policies: {policies}"
        assert len(set(policy_mapping.get(p) for p in policies)) == 1, f"Retention period is not unique: {policies}"
        return next(iter(set(policy_mapping.get(p) for p in policies)))

    @task_group
    def verify_alias(base_alias, aliases):

        @task
        def verify_write_aliases(alias, alias_list, num_months):
            regex = regex_mapping["write"]

            write_aliases= {alias['alias']: alias["index"] for alias in alias_list if
                    regex.fullmatch(alias['alias']) and alias["is_write_index"]}

            today = datetime.date.today()
            start_date = today.replace(day=1) + relativedelta(months=1)

            missing_aliases = []
            for monthly_alias in monthly_aliases(alias, start_date, num_months):
                if monthly_alias not in write_aliases:
                    missing_aliases.append(monthly_alias)
                # assert monthly_alias in alias_list, f"Missing alias '{monthly_alias}'"
            assert len(missing_aliases) == 0, f"Missing aliases: {missing_aliases}"

        retention = retention_from_policies(policies=fetch_policies(alias=base_alias))

        verify_write_aliases(alias=base_alias, alias_list=aliases, num_months=retention)

    verify_alias.partial().expand_kwargs(group_aliases_by_base(fetch_aliases()))


es_poc_dag()
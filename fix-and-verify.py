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
    dag_display_name="SPF ES POC",
    tags=["spf", "test", "poc"],
    description="This is a POC",
    catchup=False,
)
def es_poc_dag():
    hook_get = HttpHook(method='GET', http_conn_id='es-wordtags')

    @task
    def fetch_aliases():
        response = hook_get.run(
            endpoint='/_cat/aliases?h=alias,index,is_write_index',
            headers={'Accept': 'application/json'},
        )
        hook_get.check_response(response)
        return [r for r in response.json() if BASE_REGEX.match(r["alias"])][:10]

    @task
    def fetch_alias_settings(alias):
        response = hook_get.run(
            endpoint=f'/{alias}/_settings/'
                     f'index.lifecycle.name,'
                     f'index.lifecycle.rollover_alias,'
                     f'index.analysis.filter.compound_capture.patterns',
            headers={'Accept': 'application/json'},
        )
        hook_get.check_response(response)
        return response.json()

    @task
    def fetch_alias_mapping(alias):
        response = hook_get.run(
            endpoint=f'/{alias}/_mapping',
            headers={'Accept': 'application/json'},
        )
        hook_get.check_response(response)
        return response.json()

    @task
    def extract_policy(settings):
        policies = set(p["settings"]["index"]["lifecycle"]["name"] for _, p in settings.items())
        assert all(p in POLICY_MAPPING for p in policies), f"Invalid policies: {policies}"
        assert len(set(POLICY_MAPPING.get(p) for p in policies)) == 1, f"Retention period is not unique: {policies}"
        return next(iter(set(POLICY_MAPPING.get(p) for p in policies)))

    @task
    def extract_mapping(settings):
        mappings = set(p["settings"]["index"]["analysis"]["filter"]["compound_capture"]["patterns"][0] for _, p in
            settings.items())
        assert all(m == "(!?[^@!@]+)@!@" for m in mappings), f"Invalid mappings: {mappings}"
        return next(iter(mappings))

    @task
    def group_aliases_by_base(aliases: list) -> list:
        grouped = defaultdict(list)
        for alias_entry in aliases:
            alias = alias_entry["alias"]
            if not any(r.fullmatch(alias) for r in REGEX_MAPPING.values()):
                continue
            grouped[BASE_REGEX.match(alias).group(0)].append(alias_entry)
        return [{"base_alias": k, "aliases": v} for k, v in grouped.items()]

    @task
    def missing_write_aliases(alias, alias_list, num_months):
        regex = REGEX_MAPPING["write"]

        write_aliases= {alias['alias']: alias["index"] for alias in alias_list if
                regex.fullmatch(alias['alias']) and alias["is_write_index"]}

        today = datetime.date.today()
        start_date = today.replace(day=1) + relativedelta(months=1)

        missing_aliases = []
        for monthly_alias in monthly_aliases(alias, start_date, num_months):
            if monthly_alias not in write_aliases:
                missing_aliases.append(monthly_alias)
        return missing_aliases

    @task_group
    def verify_alias(base_alias, aliases):
        settings = fetch_alias_settings(alias=base_alias)
        mapping = extract_mapping(settings=settings)
        retention = extract_policy(settings=settings)
        missing_write_aliases(alias=base_alias, alias_list=aliases, num_months=retention)


    verify_alias.partial().expand_kwargs(group_aliases_by_base(fetch_aliases()))


es_poc_dag()
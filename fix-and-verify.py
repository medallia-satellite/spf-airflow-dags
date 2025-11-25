import re
from collections import defaultdict
from pprint import pprint

from airflow.decorators import task, dag, task_group
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from click import get_current_context

base_pattern = r"(\w+)_topic-builder(-\w+)+(\.\w{2,4}){0,2}(\.\w+)(\.\w{2,4}){1,2}-\1"
base_regex = re.compile(base_pattern)

regex_mapping = {
    "read": re.compile(rf"^{base_pattern}$"),
    "write": re.compile(rf"^{base_pattern}" + r"-[0-9]{4}-[0-9]{2}-[0-9]{2}$"),
    "rollover": re.compile(rf"^{base_pattern}-rollover$"),
}

@dag(
    dag_display_name="SPF ES POC",
    tags=["spf", "test", "poc"],
    description="This is a POC",
    catchup=False,
)
def es_poc_dag():

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
    def alias_group(base_alias, aliases):
        @task
        def print_input(b, a):
            print(b)
            pprint(a)

        @task
        def fetch_policy(alias: str):
            hook = HttpHook(method='GET', http_conn_id='es-wordtags')
            response = hook.run(
                endpoint=f'/{alias}/_settings/index.lifecycle.name',
                headers={'Accept': 'application/json'},
                response_check=lambda r: r.status_code == 200,
                response_filter=lambda r: set(p["settings"] for _, p in r.json().items()),
            )
            return response


        @task()
        def group_aliases() -> dict:
            parsed = {t: [] for t in regex_mapping.keys()}
            for alias_entry in aliases:
                for t, regex in regex_mapping.items():
                    if regex.fullmatch(alias_entry["alias"]):
                        parsed[t].append(
                            {alias_entry['alias'], alias_entry['index'], alias_entry['is_write_index']})
                        break
            return parsed

        fetch_policy(alias=base_alias)
        print_input(base_alias, aliases)
        return group_aliases()

    grouped = group_by_base_alias(fetch_data.output)
    alias_group.partial().expand(
        base_alias=grouped.map(lambda x: x["base_alias"]),
        aliases=grouped.map(lambda x: x["aliases"]),
    )


es_poc_dag()
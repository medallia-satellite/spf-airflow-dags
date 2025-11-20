import re
from collections import defaultdict
from pprint import pprint

from airflow.decorators import task, dag
from airflow.providers.http.operators.http import SimpleHttpOperator

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

    def filter_response(response) -> dict:
        filtered = defaultdict(list)
        for alias_entry in response:
            alias = alias_entry["alias"]
            if not any(r.fullmatch(alias) for r in regex_mapping.values()):
                continue
            filtered[base_regex.match(alias).group(0)].append(alias_entry)
        return filtered

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
    def parse_response(input_data: dict) -> dict:
        parsed = {k: {"read": [], "rollover": [], "write": []} for k in input_data.keys()}
        for k, r in input_data.items():
            for alias_entry in r:
                for t, regex in regex_mapping.items():
                    if regex.fullmatch(alias_entry["alias"]):
                        parsed[k][t].append({alias_entry['alias'], alias_entry['index'], alias_entry['is_write_index']})
                        break
        return parsed

    @task()
    def parse_res1ponse(input_data: dict) -> dict:
        return {
            key: {
                t: {
                    (entry["alias"], entry["index"], entry["is_write_index"])
                    for entry in entries
                    if regex.fullmatch(entry["alias"])
                }
                for t, regex in regex_mapping.items()
            }
            for key, entries in input_data.items()
        }

    @task()
    def aaaaaaa(item_data):
        item, data = item_data
        print(f"Processing: {item}")
        pprint(data)

    aaaaaaa.expand(item_data=parse_response(fetch_data.output))
# find_missing_months


es_poc_dag()
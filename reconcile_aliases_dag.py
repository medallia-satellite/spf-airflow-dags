from collections import defaultdict

from airflow.decorators import dag
from airflow.providers.http.hooks.http import HttpHook

from repo.elasticsearch_client import *
from repo.fix_and_verify import *
from repo.utils import *


@dag(
    dag_display_name="Reconcile Aliases",
    tags=["spf", "test", "poc"],
    description="This DAG ensures that each index has read, write and rollover aliases.",
    max_active_runs=1,
    catchup=False,
)
def reconcile_aliases_dag():
    @task
    def group_by_index(aliases: list):
        result = defaultdict(list)
        for alias_entry in aliases:
            index = alias_entry["index"]
            alias = alias_entry["alias"]
            if not INDEX_REGEX.fullmatch(index):
                continue
            if not any(r.fullmatch(alias) for r in ALIAS_REGEX_MAPPING.values()):
                continue
            result[index].append(alias)
        return [success(key=k, value=v) for k, v in result.items()]


    @task
    def check_indices_with_3_aliases(aliases: Result):
        aliases = aliases
        if len(aliases["value"]) < 3 :
            return failure(key=aliases["key"], error=f"Too few aliases: {aliases["value"]}")
        else:
            return success(key=aliases["key"], value="")


    hook_get = HttpHook(method='GET', http_conn_id='es-wordtags')
    fetched = fetch_aliases(hook=hook_get)
    grouped = group_by_index.expand(fetched)
    return push(filter_errors(check_indices_with_3_aliases.expand(grouped)))

reconcile_aliases_dag()

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
    def group_by_index_and_filter(aliases: list):
        result = defaultdict(list)
        for alias_entry in aliases:
            index = alias_entry["index"]
            alias = alias_entry["alias"]
            if not INDEX_REGEX.fullmatch(index):
                continue
            if not any(r.fullmatch(alias) for r in ALIAS_REGEX_MAPPING.values()):
                continue
            result[index].append(alias)
        return [success(key=k, value=v) for k, v in result.items() if len(v) < 3]

    hook_get = HttpHook(method='GET', http_conn_id='es-wordtags')
    fetched = fetch_aliases(hook=hook_get)
    grouped = group_by_index_and_filter(aliases=fetched)
    return push(filter_errors(grouped))

reconcile_aliases_dag()

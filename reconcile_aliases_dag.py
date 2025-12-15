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
    hook_get = HttpHook(method='GET', http_conn_id='es-wordtags')

    @task
    def collect_incomplete_alias_sets(aliases: list):
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

    @task
    def generate_actions(aliases: Result):
        index = aliases["key"]
        missing = set(generate_aliases(index).values()) - set(aliases["value"])
        actions = [{"index": index, "alias": alias,  "is_write_index": is_write_alias(alias)} for alias in missing]

        if actions:
            return success(key=index, value=actions)

        return failure(key=index, error=f"No missing aliases: {aliases['value']}")

    fetched = fetch_aliases(hook=hook_get)
    collected = collect_incomplete_alias_sets(aliases=fetched)
    return add_aliases.partial(hook=hook_get).expand(actions=generate_actions.expand(aliases=collected))

reconcile_aliases_dag()

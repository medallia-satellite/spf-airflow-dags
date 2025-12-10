import json
from typing import List

from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook

from repo.fix_and_verify import *
from repo.utils import *


@task
def fetch_aliases(hook: HttpHook):
    response = hook.run(
        endpoint='/_cat/aliases?h=alias,index,is_write_index',
        headers={'Accept': 'application/json'},
    )
    hook.check_response(response)
    return [r for r in response.json() if BASE_REGEX.match(r["alias"])]


@task
def fetch_indices(hook: HttpHook):
    response = hook.run(
        endpoint='/_cat/indices?h=index&format=json',
        headers={'Accept': 'application/json'},
    )
    hook.check_response(response)
    return [r["index"] for r in response.json() if INDEX_REGEX.match(r["index"])]


@task(task_id="fetch")
def fetch_alias_settings(hook: HttpHook, data: Result):
    alias = data["key"]
    response = hook.run(
        endpoint=f'/{alias}/_settings/'
                 f'index.lifecycle.name,'
                 f'index.lifecycle.rollover_alias',
        headers={'Accept': 'application/json'},
    )
    hook.check_response(response)
    return success(key=alias, value=list(response.json().values()))


@task(task_id="fetch")
def fetch_alias_mappings(hook: HttpHook, data: Result):
    alias = data["key"]
    response = hook.run(
        endpoint=f'/{alias}/_mapping',
        headers={'Accept': 'application/json'},
    )
    hook.check_response(response)
    return success(key=alias, value=list(response.json().values()))


@task
def create_index(hook: HttpHook, index_name: str):
    return success(key=index_name, value=json.dumps(default_index_settings_and_mappings()))
    response = hook.run(
        endpoint=f'/{index_name}',
        headers={'Content-Type': 'application/json'},
        data=json.dumps(default_index_settings_and_mappings())
    )
    hook.check_response(response)
    return response.json()

@task
def add_aliases(hook: HttpHook, actions: List[dict]):
    return success(key=actions[0]["index"], value=actions)
    response = hook.run(
        endpoint=f'/_aliases',
        headers={'Content-Type': 'application/json'},
        data=json.dumps({"actions": actions})
    )
    hook.check_response(response)
    return response.json()

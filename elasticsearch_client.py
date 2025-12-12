import json
from typing import List

from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook

from repo.fix_and_verify import *
from repo.utils import *


@task(task_id="fetch")
def fetch_aliases(hook: HttpHook):
    response = hook.run(
        endpoint='/_cat/aliases?h=alias,index,is_write_index',
        headers={'Accept': 'application/json'},
    )
    hook.check_response(response)
    return [r for r in response.json() if BASE_REGEX.match(r["alias"])]


@task(task_id="fetch")
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
def fetch_index_templates(hook: HttpHook, data: Result):
    index_template = f'{data["key"]}-rollover'
    response = hook.run(
        endpoint=f'/_index_template/{index_template}',
        headers={'Accept': 'application/json'},
    )
    hook.check_response(response)
    return success(key=data["key"], value=list(response.json()["index_templates"][0]))


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
def add_aliases(hook: HttpHook, actions: Result):
    return success(key=actions["value"][0]["index"], value=actions["value"])
    response = hook.run(
        endpoint=f'/_aliases',
        headers={'Content-Type': 'application/json'},
        data=json.dumps({"actions": actions["value"]})
    )
    hook.check_response(response)
    return response.json()


@task
def reconcile_aliases(hook: HttpHook, data: Result):
    index = data["key"]
    aliases = generate_aliases(index)
    actions = [
        {
            "add": {
                "index": index,
                "alias": aliases["read"],
                "is_write_index": False,
            }
        },
        {
            "add": {
                "index": index,
                "alias": aliases["write"],
                "is_write_index": True,
            }
        },
        {
            "add": {
                "index": index,
                "alias": aliases["rollover"],
                "is_write_index": False,
            }
        },

    ]
    response = hook.run(
        endpoint=f'/_aliases',
        headers={'Content-Type': 'application/json'},
        data=json.dumps({"actions": actions})
    )
    hook.check_response(response)
    return response.json()

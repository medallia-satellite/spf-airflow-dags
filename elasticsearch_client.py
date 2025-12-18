import json
from typing import List

from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook

from repo.fix_and_verify import *
from repo.utils import *


def xcom_pull(task_id: str, key: str) -> Any:
    context = get_current_context()
    ti = context["ti"]
    print(f"xcom_pull {task_id} {key}")
    return ti.xcom_pull(task_ids=task_id, key=key)

def xcom_push(key: str, value: Any) -> None:
    context = get_current_context()
    ti = context["ti"]
    print(f"xcom_push {key} {value}")
    ti.xcom_push(key, value)


def fetch_from_endpoint(hook: HttpHook, endpoint: str):
    response = hook.run(
        endpoint=endpoint,
        headers={'Accept': 'application/json'},
    )
    hook.check_response(response)
    return response.json()


#########################################
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
def fetch_index_template(hook: HttpHook, data: Result):
    index_template = f'{data["key"]}-rollover'
    response = hook.run(
        endpoint=f'/_index_template/{index_template}',
        headers={'Accept': 'application/json'},
    )
    hook.check_response(response)
    return success(key=data["key"], value=response.json()["index_templates"][0])


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

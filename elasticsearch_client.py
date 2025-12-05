import json

from airflow.decorators import task

from repo.fix_and_verify import BASE_REGEX, INDEX_REGEX, INDEX_SETTINGS_AND_MAPPINGS, generate_aliases
from repo.utils import success


@task
def fetch_aliases(hook):
    response = hook.run(
        endpoint='/_cat/aliases?h=alias,index,is_write_index',
        headers={'Accept': 'application/json'},
    )
    hook.check_response(response)
    return [r for r in response.json() if BASE_REGEX.match(r["alias"])]


@task
def fetch_indices(hook):
    response = hook.run(
        endpoint='/_cat/indices?h=index&format=json',
        headers={'Accept': 'application/json'},
    )
    hook.check_response(response)
    return [r["index"] for r in response.json() if INDEX_REGEX.match(r["index"])]


@task(task_id="fetch")
def fetch_alias_settings(hook, data):
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
def fetch_alias_mappings(hook, data):
    alias = data["key"]
    response = hook.run(
        endpoint=f'/{alias}/_mapping',
        headers={'Accept': 'application/json'},
    )
    hook.check_response(response)
    return success(key=alias, value=list(response.json().values()))


@task
def create_index(hook, index_name):
    return success(key=index_name, value=json.dumps(INDEX_SETTINGS_AND_MAPPINGS))
    response = hook.run(
        endpoint=f'/{index_name}',
        headers={'Content-Type': 'application/json'},
        data=json.dumps(INDEX_SETTINGS_AND_MAPPINGS)
    )
    hook.check_response(response)
    return response.json()


@task
def add_aliases(hook, data):
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
    return success(key=data["key"], value=actions)
    response = hook.run(
        endpoint=f'/_aliases',
        headers={'Content-Type': 'application/json'},
        data=json.dumps({"actions": actions})
    )
    hook.check_response(response)
    return success(key=data["key"], value=response.json())

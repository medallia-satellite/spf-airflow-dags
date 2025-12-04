import functools
import json
from typing import TypedDict, Optional, Any

from airflow.decorators import task
from airflow.operators.python import get_current_context


class Result(TypedDict):
    success: bool
    key: str
    error: Optional[str]
    value: Optional[Any]

def success(key: str, value: Any) -> Result:
    return Result(success=True, key=key, error=None, value=value)

def failure(key: str, error: str) -> Result:
    return Result(success=False, key=key, error=error, value=None)

def chain_on_success(func):
    """
    Decorator for functional pipelines.

    It checks the 'success' key in the single positional argument (data dict).
    If 'data["success"]' is False, it immediately returns the data dictionary,
    short-circuiting the execution chain.
    """

    @functools.wraps(func)
    def wrapper(data):
        # Check for the failure condition at the beginning of the function
        if not data["success"]:
            return data

        # If success is True, execute the decorated function
        return func(data)

    return wrapper

@task
def filter_errors(data):
    results = [d for d in data if d["success"]]
    print(f"Collected {len(results)} successes.")
    return results

@task
def filter_success(data):
    results = [success(key=d["key"], value=d["error"]) for d in data if not d["success"]]
    print(f"Filtering {len(data) - len(results)} successes.")
    return results

@task
def filter_empty(data):
    results = [d for d in data if d["success"] and d["value"]]
    print(f"Filtering {len(data) - len(results)} empty successes.")
    return results

@task
def print_errors(data):
    results = {d['key']: d['error'] for d in data if not d["success"]}
    print(f"Errors found: {len(results)}")
    print(json.dumps(results, indent=2))
    return results

@task(task_id="push")
def push(data):
    context = get_current_context()
    ti = context["ti"]
    for d in data:
        if d["success"]:
            ti.xcom_push(d["key"], d["value"])

    return [success(key=d["key"], value=None) for d in data if d["success"]]


def retrieve(stage, key):
    context = get_current_context()
    ti = context["ti"]
    return ti.xcom_pull(task_ids=f"{stage}.push", key=key)

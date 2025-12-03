import json
from typing import TypedDict, Optional, Any

from airflow.decorators import task
from airflow.operators.python import get_current_context


class Result(TypedDict):
    success: bool
    instance: str
    error: Optional[str]
    value: Optional[Any]


def success(instance: str, value: Any) -> Result:
    return Result(success=True, instance=instance, error=None, value=value)


def failure(instance: str, error: str) -> Result:
    return Result(success=False, instance=instance, error=error, value=None)


@task
def filter_errors(input_data):
    results = [d for d in input_data if d["success"]]
    print(f"Collected {len(results)} successes.")
    return results


@task
def filter_empty(input_data):
    results = [d for d in input_data if d["success"] and d["value"]]
    print(f"Filtering {len(input_data) - len(results)} empty successes.")
    return results


@task
def print_errors(input_data):
    results = {d['instance']: d['error'] for d in input_data if not d["success"]}
    print(f"Errors found: {len(results)}")
    print(json.dumps(results, indent=2))
    return results


@task(task_id="push")
def push(input_data):
    context = get_current_context()
    ti = context["ti"]
    for d in input_data:
        if d["success"]:
            ti.xcom_push(d["instance"], d["value"])

    return [success(instance=d["instance"], value=None) for d in input_data if d["success"]]


def retrieve(stage, instance):
    context = get_current_context()
    ti = context["ti"]
    return ti.xcom_pull(task_ids=f"{stage}.push", key=instance)

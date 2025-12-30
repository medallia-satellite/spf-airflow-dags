from typing import Any

from airflow.operators.python import get_current_context
from airflow.providers.http.hooks.http import HttpHook


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


def http_hook_put(conn_id: str, endpoint: str, data: str):
    hook_put = HttpHook(method="PUT", http_conn_id=conn_id)
    response = hook_put.run(
        endpoint=f"/{endpoint}?pretty",
        headers={"Content-Type": "application/json"},
        data=data,
    )
    hook_put.check_response(response)
    return response.json()


def http_hook_post(conn_id: str, endpoint: str, data: str):
    hook_post = HttpHook(method="POST", http_conn_id=conn_id)
    response = hook_post.run(
        endpoint=f"/{endpoint}?pretty",
        headers={"Content-Type": "application/json"},
        data=data,
    )
    hook_post.check_response(response)
    return response.json()


def http_hook_get(conn_id: str, endpoint: str):
    hook_get = HttpHook(method="GET", http_conn_id=conn_id)
    response = hook_get.run(
        endpoint=endpoint,
        headers={"Accept": "application/json"},
    )
    hook_get.check_response(response)
    return response.json()

import functools
from typing import Any, TypedDict, Optional, List

from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.http.hooks.http import HttpHook


def param_value(param: str) -> Any:
    ctx = get_current_context()
    return  ctx["params"][param]


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


def http_hook_post(conn_id: str, endpoint: str, data: str, params: Optional[dict] = None) -> Any:
    hook_post = HttpHook(method="POST", http_conn_id=conn_id)
    response = hook_post.run(
        endpoint=f"/{endpoint}?pretty",
        headers={"Content-Type": "application/json"},
        params=params,
        data=data,
    )
    hook_post.check_response(response)
    return response.json()


def http_hook_get(conn_id: str, endpoint: str, params: Optional[dict] = None):
    hook_get = HttpHook(method="GET", http_conn_id=conn_id)
    response = hook_get.run(
        endpoint=endpoint,
        data=params,
        headers={"Accept": "application/json"},
    )
    hook_get.check_response(response)
    return response.json()


def chain_on_success(func):
    @functools.wraps(func)
    def wrapper(context):
        if not context["success"]:
            return context
        return func(context)

    return wrapper


def chain_on_error_in_stage(stage):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(context):
            if context["success"] or context["stage"] != stage:
                return context
            return func(context)

        return wrapper

    return decorator


class Context(TypedDict, total=False):
    tenant: str
    tenant_id: int
    success: bool
    error: Optional[Any]
    stage: Optional[str]
    value: Optional[Any]
    retention: Optional[int]
    latest_suffix: Optional[int]


def success(
    context: Context,
    stage: str,
    value: Any = None,
) -> Context:
    context.update(
        {
            "success": True,
            "stage": stage,
            "value": value,
            "error": None,
        }
    )
    return context


def failure(
    context: Context,
    stage: str,
    error: Any = None,
) -> Context:
    context.update(
        {
            "success": False,
            "stage": stage,
            "value": None,
            "error": error,
        }
    )
    return context


@task(trigger_rule="none_failed")
def wait_for_completion(upstream: List[Context]) -> List[Context]:
    return upstream


@task
def select_eligible_for_fix(upstream: List[Context], stage: str) -> List[Context]:
    eligible_for_fix = []
    for i, c in enumerate(upstream):
        if not c["success"] and c["stage"] == stage:
            print(f"{i}: {c['tenant']} - {c['error']}")
            eligible_for_fix.append(c)
    total = len(upstream)
    errors = sum(1 for c in upstream if not c["success"])
    print(
        f"""

        Summary
            total success: {total - errors}/{total}
            total errors: {errors}/{total}
                errors in stage '{stage}': {len(eligible_for_fix)}/{errors}
        """
    )
    return eligible_for_fix

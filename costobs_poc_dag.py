from datetime import datetime
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook

@dag(
    dag_display_name="Cost Observability POC",
    max_active_runs=1,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    render_template_as_native_obj=True,
)
def costobs_poc_dag():
    @task
    def task_a() -> List[Dict[str, Any]]:
        hook_get = HttpHook(method="GET", http_conn_id="sharedservices-elasticsearch")
        response = hook_get.run(
            endpoint="/seaas-system-*/_stats",
        )
        hook_get.check_response(response)
        return response.json()

    task_a()

costobs_poc_dag()

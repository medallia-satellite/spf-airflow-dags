from datetime import datetime
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook

MEMORY_FIELDS = [
    ("segments", "memory_in_bytes"),
    ("segments", "index_writer_memory_in_bytes"),
    ("segments", "version_map_memory_in_bytes"),
    ("segments", "fixed_bit_set_memory_in_bytes"),
    ("fielddata", "memory_size_in_bytes"),
    ("query_cache", "memory_size_in_bytes"),
    ("request_cache", "memory_size_in_bytes"),
]

"""Per-index CPU, memory, and storage from GET /_stats.
cpu_ms is cumulative since the shards started, so it reflects the total so far, not the time spent in the last run.
"""

def summarize(stats_response: dict) -> dict:
    out = {}
    for index, s in stats_response["indices"].items():
        t = s.get("total", {})  # includes replicas
        get = lambda sec, f: t.get(sec, {}).get(f, 0)
        out[index] = {
            "cpu_ms": get("search", "query_time_in_millis")
                      + get("search", "fetch_time_in_millis")
                      + get("indexing", "index_time_in_millis"),
            "memory_bytes": sum(get(sec, f) for sec, f in MEMORY_FIELDS),
            "storage_bytes": get("store", "size_in_bytes") + get("translog", "size_in_bytes"),
        }
    return out


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
            endpoint="/_all/_stats",
        )
        hook_get.check_response(response)
        summary = summarize(response.json())
        print(sum(m['memory_bytes'] for m in summary.values()))
        return summary

    task_a()

costobs_poc_dag()

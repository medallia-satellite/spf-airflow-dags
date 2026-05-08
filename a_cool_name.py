from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    dag_display_name="A cool DAG name",
    tags=["spf", "elasticsearch"],
    description="This nice DAG triggers other dags",
    max_active_runs=1,
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
)
def a_cool_dag():
    trigger_child = TriggerDagRunOperator(
        task_id="trigger_fix_monthly_indices_dag",
        trigger_dag_id="fix_and_verify_dag",  # The DAG ID to trigger
        wait_for_completion=True,  # Wait for the child DAG to finish
        poke_interval=30,
        conf={
            "conn_id": "es-wordtags",
            "dry_run": True,
        }
    )

a_cool_dag()

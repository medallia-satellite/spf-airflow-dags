from airflow.decorators import dag
from airflow.models import Variable
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
def orchestrate_fix_and_ilm_dag():
    targets = Variable.get(
        "orchestrate_dag_targets",
        deserialize_json=True,
    )
    for target, config in targets.items():
        f = TriggerDagRunOperator(
            task_id=f"repair_and_validate_indices__{target}",
            trigger_dag_id="repair_and_validate_indices_dag",
            wait_for_completion=True,
            poke_interval=30,
            conf=config
        )
        i = TriggerDagRunOperator(
            task_id=f"unblock_ilm_retention__{target}",
            trigger_dag_id="unblock_ilm_retention_dag",  # The DAG ID to trigger
            wait_for_completion=True,  # Wait for the child DAG to finish
            poke_interval=30,
            conf=config
        )
        f >> i

orchestrate_fix_and_ilm_dag()

from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    dag_display_name="Orchestrate Maintenance DAG",
    tags=["spf", "elasticsearch"],
    description="This DAG uses 'orchestrate_dag_targets' as targets to trigger maintenance workflows.",
    max_active_runs=1,
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
)
def orchestrate_maintenance_dag():
    targets = Variable.get(
        "orchestrate_dag_targets",
        deserialize_json=True,
    )
    for target, config in targets.items():
        t1 = TriggerDagRunOperator(
            task_id=f"reconcile_wordtags_indices__{target}",
            trigger_dag_id="reconcile_wordtags_indices_dag",
            wait_for_completion=True,
            poke_interval=30,
            conf=config
        )
        t2 = TriggerDagRunOperator(
            task_id=f"finalize_expired_indices__{target}",
            trigger_dag_id="finalize_expired_indices_dag",
            wait_for_completion=True,
            poke_interval=30,
            conf=config
        )
        t1 >> t2

orchestrate_maintenance_dag()

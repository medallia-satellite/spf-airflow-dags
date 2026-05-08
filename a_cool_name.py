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
    f1 = TriggerDagRunOperator(
        task_id="f_wordtags",
        trigger_dag_id="fix_and_verify_dag",  # The DAG ID to trigger
        wait_for_completion=True,  # Wait for the child DAG to finish
        poke_interval=30,
        conf={
            "conn_id": "es-wordtags",
            "dry_run": True,
        }
    )

    f2 = TriggerDagRunOperator(
        task_id="f_wordtags_client_sandbox",
        trigger_dag_id="fix_and_verify_dag",  # The DAG ID to trigger
        wait_for_completion=True,  # Wait for the child DAG to finish
        poke_interval=30,
        conf={
            "conn_id": "es-wordtags-client-sandbox",
            "dry_run": True,
        }
    )



    i1 = TriggerDagRunOperator(
        task_id="i_wordtags",
        trigger_dag_id="ilm_keeper_dag",  # The DAG ID to trigger
        wait_for_completion=True,  # Wait for the child DAG to finish
        poke_interval=30,
        conf={
            "conn_id": "es-wordtags",
            "dry_run": True,
        }
    )


    i2 = TriggerDagRunOperator(
        task_id="i_wordtags_client_sandbox",
        trigger_dag_id="ilm_keeper_dag",  # The DAG ID to trigger
        wait_for_completion=True,  # Wait for the child DAG to finish
        poke_interval=30,
        conf={
            "conn_id": "es-wordtags-client-sandbox",
            "dry_run": True,
        }
    )


    f1 >> i1 >> f2 >> i2

a_cool_dag()

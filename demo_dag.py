from airflow.decorators import dag, task
from airflow.models import Param

@dag(
    dag_display_name="Demo",
    tags=["spf", "demo"],
    description="This the DAG description",
    max_active_runs=1,
    catchup=False,
    params={
        "name": Param("stranger", type="string")
    },
)
def demo_dag():
    @task
    def say_hello(n: str):
        print(f"hello {n}")

    @task
    def say_bye(n: str):
        print(f"bye {n}")

    name = "{{ params.name }}"
    say_hello(name) >> say_bye(name)

demo_dag()

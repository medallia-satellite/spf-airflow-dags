from airflow.decorators import dag, task

@dag(
    dag_display_name="Demo",
    tags=["spf", "demo"],
    description="This the DAG description",
    max_active_runs=1,
    catchup=False
)
def demo_dag():
    @task
    def say_hello():
        print("hello world")

    say_hello()

demo_dag()

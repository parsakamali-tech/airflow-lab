from datetime import datetime
from airflow.decorators import dag, task

@dag(
    dag_id="hello_taskflow",
    start_date=datetime(2025, 1, 1),
    schedule= None,
    catchup=False,
    tags=["intro"],
)
def hello_pipeline():
    @task
    def say_hello():
        print("Hello from Airflow TaskFlow!")

    say_hello()

hello_pipeline()

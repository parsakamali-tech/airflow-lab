from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.base import BaseHook
# from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators.bash import BashOperator

@dag(
    dag_id="vars_conns_demo",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["basics", "config"],
)
def vars_conns_demo():

    @task
    def show_variable():
        env = Variable.get("etl_env", default_var="dev")
        print(f"[VAR] etl_env = {env}")
        return env

    @task
    def show_connection():
        conn = BaseHook.get_connection("demo_conn")
        # مراقب لو دادن پسورد باش!
        print(f"[CONN] id=demo_conn host={conn.host} schema={conn.schema or ''} login={conn.login or ''}")
        print("[CONN] (password is NOT logged)")
        return {"host": conn.host, "schema": conn.schema}

    # نمونه‌ی تمپلیتینگ با Jinja (بدون Provider):
    echo_template = BashOperator(
        task_id="echo_template",
        bash_command=(
            'echo "run_id={{ run_id }} ds={{ ds }} etl_env={{ var.value.etl_env | default(\'dev\') }}"'
        ),
    )

    v = show_variable()
    c = show_connection()
    v >> echo_template  # اول Variable رو چاپ کن، بعد echo_template

vars_conns_demo()

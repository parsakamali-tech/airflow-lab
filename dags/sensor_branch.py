from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.sensors.python import PythonSensor
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

FLAG_PATH = Path("/opt/input/ready.flag")

def file_is_ready():
    return FLAG_PATH.exists() # True اگر فایل وجود داشت

@dag(
    dag_id="sensor_branch_demo",
    start_date=datetime(2025, 1, 1),
    schedule=None,         # دستی تریگر کن
    catchup=False,
    tags=["sensor", "branching"],
)
def sensor_branch_demo():
    wait_for_file = PythonSensor(
        task_id="wait_for_ready_flag",
        python_callable=file_is_ready,
        poke_interval=15,           # هر 15 ثانیه چک کن
        timeout=60 * 60,            # حداکثر 1 ساعت صبر کن
        mode="reschedule",          # Worker اشغال نشه
        soft_fail=False,            # اگر True باشه، بعد timeout به‌جای Fail، Skip می‌شه
    )

    def choose_path():
        env = Variable.get("etl_env", default_var="dev")
        return "process" if env == "dev" else "skip"

    branch = BranchPythonOperator(
        task_id="branch_on_env",
        python_callable=choose_path,
    )

    process = EmptyOperator(task_id="process")
    skip = EmptyOperator(task_id="skip")
    done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")

    wait_for_file >> branch
    branch >> process >> done
    branch >> skip >> done

sensor_branch_demo()

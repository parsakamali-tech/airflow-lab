from datetime import datetime
import os, logging
from airflow.decorators import dag
from airflow.sensors.python import PythonSensor
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

FLAG = "/opt/airflow/input/ready.flag"

def diag_file_is_ready():
    exists = os.path.exists(FLAG)
    # لاگِ تشخیصی: وضعیت فایل و محتویات دایرکتوری
    try:
        listing = os.listdir("/opt/input")
    except Exception as e:
        listing = f"<ERR listdir: {e}>"
    logging.info("[DIAG] exists=%s  whoami=%s  ls(/opt/input)=%s", exists, os.geteuid(), listing)
    return exists

@dag(
    dag_id="sensor_branch_demo",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
)
def sensor_branch_demo():
    wait = PythonSensor(
        task_id="wait_for_ready_flag",
        python_callable=diag_file_is_ready,
        poke_interval=10,
        timeout=3600,
        mode="reschedule",  # منابع رو اشغال نکنه
    )

    def choose_path():
        return "process" if Variable.get("etl_env", default_var="dev") == "dev" else "skip"

    branch = BranchPythonOperator(task_id="branch_on_env", python_callable=choose_path)
    process = EmptyOperator(task_id="process")
    skip = EmptyOperator(task_id="skip")
    done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")

    wait >> branch
    branch >> process >> done
    branch >> skip >> done

sensor_branch_demo()

from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

@dag(
    dag_id="tg_minimal",
    start_date=datetime(2025, 1, 1),
    schedule=None,      # دستی اجراش می‌کنیم
    catchup=False,
    tags=["taskgroup", "intro"],
)
def tg_minimal():
    # یک گروه به اسم etl می‌سازیم
    with TaskGroup(group_id="etl") as etl:
        @task
        def extract():
            # فقط یه نمونهٔ ساده
            return {"rows": 100}

        @task
        def transform(payload: dict):
            # مثلا ۱۰٪ داده‌ها فیلتر شده
            payload["rows"] = int(payload["rows"] * 0.9)
            return payload

        @task
        def load(payload: dict):
            print(f"[load] loaded {payload['rows']} rows")

        # ترتیب داخل گروه: extract -> transform -> load
        load(transform(extract()))

    # خارج از گروه فعلاً چیزی نداریم؛ همین که group درست نمایش داده بشه کافی‌ه
    etl

tg_minimal()

from datetime import datetime
from statistics import mean
from airflow.decorators import dag, task

@dag(
    dag_id="xcom_intro",
    start_date=datetime(2025, 1, 1),
    schedule=None,          # دستی تریگر می‌کنیم
    catchup=False,
    tags=["intro", "xcom"],
)
def xcom_pipeline():

    @task
    def produce_numbers():
        # ورودی نمونه؛ می‌تونی بعداً تغییرش بدی
        nums = [12, 18, 15, 21, 24]
        return nums  # این مقدار از طریق XCom به تسک بعدی می‌رسه

    @task
    def calc_avg(nums: list):
        return float(mean(nums))

    @task
    def report(avg: float):
        print(f"[REPORT] Average = {avg}")

    numbers = produce_numbers()
    average = calc_avg(numbers)
    report(average)

xcom_pipeline()

"""
Lab 4 - copy to dags/team_<yourname>.py and complete the capstone.

Mandatory:
  - >= 5 Airflow tasks in your dag
  - 3 Spark transforms in include/team_<yourname>_spark.py
  - Try to be creative with the tasks

Steps:
  1. Change dag_id below.
  2. Copy include/team_spark_TEMPLATE.py -> include/team_<yourname>_spark.py
  3. Define 5 tasks
  4. Wire spark task to YOUR run_daily() in include/team_<yourname>_spark.py
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor

from include.ingest import ingest_day, validate_silver
from include.paths import report_json
from include.team_spark_Guillaume_Yassine import run_daily

# TODO: after creating team_<yourname>_spark.py, import run_daily from there:
# from include.team_<yourname>_spark import run_daily

DEFAULT_ARGS = {
    "owner": "team",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def notify_failure(context):
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    ds = context["ds"]

    print(f"[ERROR ALERT] DAG={dag_id} TASK={task_id} FAILED for ds={ds}")


with DAG(
    dag_id="team_Guillaume_Yassine",
    description="Capstone retail KPI pipeline",
    start_date=datetime(2026, 6, 1),
    end_date=datetime(2026, 6, 14),
    schedule="@daily",
    catchup=True,
    default_args=DEFAULT_ARGS,
    tags=["lab4", "capstone"],
) as dag:
    wait_csv = FileSensor(
        task_id="wait_for_file",
        filepath="incoming/transactions_{{ ds }}.csv",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=600,
        mode="reschedule",
    )

    # 2. INGEST SILVER
    @task
    def ingest(ds: str):
        ingest_day(logical_date=ds)

    ingest_silver = ingest()

    # 3. VALIDATION TASK
    @task(retries=0, on_failure_callback=notify_failure)
    def validate(ds: str):
        validate_silver(logical_date=ds)

    validate_data = validate()

    # 4. SPARK TASK
    @task
    def run_spark(ds: str):
        run_daily(ds)

    run_spark_kpis = run_spark()

    # 5. PUBLISH TASK
    @task
    def publish(ds: str):
        path = report_json(ds)
        print(f"Dashboard available at: {path}")

    publish_dashboard = publish()

    # OPTIONAL 6. NOTIFY TASK
    @task
    def notify(ds: str):
        print(f"[SUCCESS] DAG team_Guillaume_Yassine finished for {ds}")

    notify_status = notify()

    # DAG DEPENDENCIES

(wait_csv >> ingest_silver >> validate_data)

validate_data >> [run_spark_kpis, notify_status]
run_spark_kpis >> publish_dashboard

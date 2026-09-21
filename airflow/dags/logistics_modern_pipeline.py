from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    "logistics_production_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["logistics", "streaming", "lakehouse", "quality", "dbt"],
) as dag:
    start = EmptyOperator(task_id="start")

    ingest = BashOperator(
        task_id="generate_batch_data",
        bash_command="python /opt/project/scripts/generate_data.py",
    )
    quality = BashOperator(
        task_id="quality_gate",
        bash_command="python /opt/project/src/quality/validate.py",
    )
    gold = BashOperator(
        task_id="build_gold",
        bash_command="python /opt/project/src/transform/build_gold.py",
    )
    load = BashOperator(
        task_id="load_postgres",
        bash_command="python /opt/project/src/ingestion/load_postgres.py",
    )
    dbt = BashOperator(
        task_id="dbt_build_and_test",
        bash_command="cd /opt/project/dbt && dbt build --profiles-dir .",
    )
    finish = EmptyOperator(task_id="pipeline_complete")

    start >> ingest >> quality >> gold >> load >> dbt >> finish

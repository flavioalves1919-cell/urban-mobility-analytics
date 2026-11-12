from datetime import datetime, timezone
from pathlib import Path
import os

from airflow.sdk import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from src.ingestion.download_yellow_taxi import (
    create_session,
    download_month,
    is_valid_parquet,
)


PROJECT_ROOT = "/home/flavio/data-engineering"


@dag(
    dag_id="urban_mobility_pipeline",
    description="Pipeline de dados NYC Yellow Taxi 2025",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["urban-mobility", "bronze", "silver", "spark"],
)
def urban_mobility_pipeline():

    @task
    def ingest_yellow_taxi():
        with create_session() as session:
            for month in range(1, 7):
                download_month(session, 2025, month)

        print("Ingestao dos seis meses concluida.")

    @task
    def audit_bronze():
        directory = Path("/opt/airflow/data/bronze/yellow_taxi")
        total_size = 0

        for month in range(1, 7):
            filename = f"yellow_tripdata_2025-{month:02d}.parquet"
            file_path = directory / filename

            if not is_valid_parquet(file_path):
                raise ValueError(
                    f"Arquivo ausente ou invalido: {filename}"
                )

            size = file_path.stat().st_size
            total_size += size

            print(
                f"VALIDADO: {filename} | "
                f"{size / 1024**2:.2f} MB"
            )

        print(
            f"Auditoria concluida. "
            f"Tamanho total: {total_size / 1024**2:.2f} MB"
        )

    load_postgres = DockerOperator(
        task_id="load_silver_postgres",
        image="urban-mobility-loader:1.0",
        command=[
            "src/silver/load_silver_postgres.py",
            "--year", "2025",
            "--start-month", "1",
            "--end-month", "6",
        ],
        docker_url="unix://var/run/docker.sock",
        mounts=[
            Mount(
                source=f"{PROJECT_ROOT}/src",
                target="/opt/urban-mobility/src",
                type="bind",
                read_only=True,
            ),
            Mount(
                source=f"{PROJECT_ROOT}/data/silver",
                target="/opt/urban-mobility/data/silver",
                type="bind",
                read_only=True,
            ),
        ],
        environment={
            "DB_HOST": os.environ["DB_HOST"],
            "DB_PORT": os.environ["DB_PORT"],
            "DB_NAME": os.environ["DB_NAME"],
            "DB_USER": os.environ["DB_USER"],
            "DB_PASSWORD": os.environ["DB_PASSWORD"],
        },
        working_dir="/opt/urban-mobility",
        mount_tmp_dir=False,
        mem_limit="1200m",
        cpus=1.0,
        auto_remove="success",
    )

    process_silver = DockerOperator(
        task_id="process_silver",
        image="urban-mobility-spark:1.0",
        command=[
            "python",
            "src/silver/transform_yellow_taxi.py",
            "--year", "2025",
            "--start-month", "1",
            "--end-month", "6",
        ],
        docker_url="unix://var/run/docker.sock",
        mounts=[
            Mount(
                source=f"{PROJECT_ROOT}/src",
                target="/opt/urban-mobility/src",
                type="bind",
                read_only=True,
            ),
            Mount(
                source=f"{PROJECT_ROOT}/data",
                target="/opt/urban-mobility/data",
                type="bind",
            ),
        ],
        working_dir="/opt/urban-mobility",
        mount_tmp_dir=False,
        mem_limit="1500m",
        cpus=1.0,
        auto_remove="success",
    )

    run_dbt_gold = DockerOperator(
        task_id="run_dbt_gold",
        image="urban-mobility-dbt:1.0",
        command=[
            "build",
            "--project-dir", "/opt/dbt/project",
            "--profiles-dir", "/root/.dbt",
        ],
        docker_url="unix://var/run/docker.sock",
        mounts=[
            Mount(
                source=f"{PROJECT_ROOT}/dbt",
                target="/opt/dbt/project",
                type="bind",
                read_only=True,
            ),
            Mount(
                source=f"{PROJECT_ROOT}/dbt/logs",
                target="/opt/dbt/project/logs",
                type="bind",
            ),
            Mount(
                source=f"{PROJECT_ROOT}/dbt/target",
                target="/opt/dbt/project/target",
                type="bind",
            ),
            Mount(
                source="/home/flavio/.dbt",
                target="/root/.dbt",
                type="bind",
                read_only=True,
            ),
        ],
        working_dir="/opt/dbt/project",
        mount_tmp_dir=False,
        mem_limit="1g",
        cpus=1.0,
        auto_remove="success",
    )

    ingestion = ingest_yellow_taxi()
    audit = audit_bronze()

    ingestion >> audit >> process_silver >> load_postgres >> run_dbt_gold


urban_mobility_pipeline()

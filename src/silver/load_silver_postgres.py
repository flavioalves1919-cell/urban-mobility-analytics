import argparse
import logging
import os
from pathlib import Path

import pyarrow.parquet as pq
import psycopg
from dotenv import dotenv_values


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "yellow_taxi"
BATCH_SIZE = 256

COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "RatecodeID",
    "store_and_fwd_flag", "PULocationID", "DOLocationID",
    "payment_type", "fare_amount", "extra", "mta_tax",
    "tip_amount", "tolls_amount", "improvement_surcharge",
    "total_amount", "congestion_surcharge", "Airport_fee",
    "cbd_congestion_fee", "rejection_reasons", "quality_flags",
    "reference_month", "source_file", "is_valid_revenue_payment",
    "quality_status",
]

COPY_SQL = """
COPY silver.yellow_taxi_trips (
    "VendorID", tpep_pickup_datetime, tpep_dropoff_datetime,
    passenger_count, trip_distance, "RatecodeID",
    store_and_fwd_flag, "PULocationID", "DOLocationID",
    payment_type, fare_amount, extra, mta_tax, tip_amount,
    tolls_amount, improvement_surcharge, total_amount,
    congestion_surcharge, "Airport_fee", cbd_congestion_fee,
    rejection_reasons, quality_flags, reference_month,
    source_file, is_valid_revenue_payment, quality_status
) FROM STDIN
"""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


def get_database_config():
    file_config = dotenv_values(PROJECT_ROOT / ".env")
    required = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]

    config = {
        key: os.getenv(key) or file_config.get(key)
        for key in required
    }

    missing = [key for key, value in config.items() if not value]
    if missing:
        raise ValueError(f"Variáveis ausentes: {', '.join(missing)}")

    return config


def load_month(config, month):
    month_dir = SILVER_DIR / f"reference_month={month}"
    files = list(month_dir.glob("*.parquet"))

    if len(files) != 1:
        raise RuntimeError(
            f"Esperado um arquivo Parquet em {month_dir}; encontrados: {len(files)}"
        )

    parquet = pq.ParquetFile(files[0])
    expected_count = parquet.metadata.num_rows

    logger.info(
        "Iniciando %s | registros esperados: %s",
        month,
        f"{expected_count:,}",
    )

    loaded_count = 0

    with psycopg.connect(
        host=config["DB_HOST"],
        port=config["DB_PORT"],
        dbname=config["DB_NAME"],
        user=config["DB_USER"],
        password=config["DB_PASSWORD"],
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM silver.yellow_taxi_trips WHERE reference_month = %s",
                (month,),
            )

            with cur.copy(COPY_SQL) as copy:
                for batch in parquet.iter_batches(
                    batch_size=BATCH_SIZE,
                    columns=COLUMNS,
                    use_threads=False,
                ):
                    arrow_columns = [
                        batch.column(index)
                        for index in range(batch.num_columns)
                    ]

                    for row_index in range(batch.num_rows):
                        copy.write_row(
                            tuple(
                                column[row_index].as_py()
                                for column in arrow_columns
                            )
                        )

                    loaded_count += batch.num_rows

                    logger.info(
                        "%s | carregados: %s / %s",
                        month,
                        f"{loaded_count:,}",
                        f"{expected_count:,}",
                    )

            if loaded_count != expected_count:
                raise RuntimeError(
                    f"Leitura divergente: esperado={expected_count}, "
                    f"lido={loaded_count}"
                )

            cur.execute(
                "SELECT COUNT(*) FROM silver.yellow_taxi_trips WHERE reference_month = %s",
                (month,),
            )
            database_count = cur.fetchone()[0]

            if database_count != expected_count:
                raise RuntimeError(
                    f"Banco divergente: esperado={expected_count}, "
                    f"encontrado={database_count}"
                )

    logger.info(
        "Mês %s concluído e validado: %s registros",
        month,
        f"{loaded_count:,}",
    )
    return loaded_count


def main():
    parser = argparse.ArgumentParser(
        description="Carga da camada Silver para PostgreSQL"
    )
    parser.add_argument("--start-month", type=int, default=1)
    parser.add_argument("--end-month", type=int, default=6)
    parser.add_argument("--year", type=int, default=2025)
    args = parser.parse_args()

    if not 1 <= args.start_month <= args.end_month <= 12:
        parser.error("Informe meses entre 1 e 12, em ordem crescente.")

    if not SILVER_DIR.exists():
        raise FileNotFoundError(f"Diretório não encontrado: {SILVER_DIR}")

    config = get_database_config()
    total_loaded = 0

    for month_number in range(args.start_month, args.end_month + 1):
        month = f"{args.year}-{month_number:02d}"
        total_loaded += load_month(config, month)

    logger.info("CARGA FINALIZADA | total: %s", f"{total_loaded:,}")


if __name__ == "__main__":
    main()

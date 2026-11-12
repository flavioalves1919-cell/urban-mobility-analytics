import argparse
import logging
import re
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "yellow_taxi"
SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "yellow_taxi"
QUARANTINE_DIR = PROJECT_ROOT / "data" / "quarantine" / "yellow_taxi"

MONEY_COLUMNS = [
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
    "cbd_congestion_fee",
]

INTEGER_COLUMNS = [
    "VendorID",
    "passenger_count",
    "RatecodeID",
    "PULocationID",
    "DOLocationID",
    "payment_type",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("UrbanMobility-Silver")
        .master("local[1]")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )


def extract_reference_month(file_path: Path) -> str:
    match = re.search(
        r"yellow_tripdata_(\d{4})-(\d{2})\.parquet$",
        file_path.name,
    )

    if not match:
        raise ValueError(
            f"Nome de arquivo fora do padrão esperado: {file_path.name}"
        )

    year, month = match.groups()

    if not 1 <= int(month) <= 12:
        raise ValueError(f"Mês inválido no arquivo: {file_path.name}")

    return f"{year}-{month}"


def standardize_types(df: DataFrame) -> DataFrame:
    for column in INTEGER_COLUMNS:
        df = df.withColumn(column, F.col(column).cast("integer"))

    for column in MONEY_COLUMNS:
        df = df.withColumn(
            column,
            F.col(column).cast("decimal(18, 2)"),
        )

    df = df.withColumn(
        "trip_distance",
        F.col("trip_distance").cast("decimal(18, 6)"),
    )

    return df


def add_quality_classification(
    df: DataFrame,
    reference_month: str,
    source_file: str,
) -> DataFrame:

    df = df.withColumn(
        "trip_duration_minutes",
        F.round(
            (
                F.unix_timestamp("tpep_dropoff_datetime")
                - F.unix_timestamp("tpep_pickup_datetime")
            ) / 60.0,
            2,
        ),
    )

    pickup_month = F.date_format(
        F.col("tpep_pickup_datetime"),
        "yyyy-MM",
    )

    rejection_conditions = [
        (
            F.col("tpep_pickup_datetime").isNull(),
            "PICKUP_DATETIME_NULL",
        ),
        (
            F.col("tpep_dropoff_datetime").isNull(),
            "DROPOFF_DATETIME_NULL",
        ),
        (
            F.col("tpep_dropoff_datetime")
            <= F.col("tpep_pickup_datetime"),
            "INVALID_TRIP_DURATION",
        ),
        (
            pickup_month != F.lit(reference_month),
            "PICKUP_OUTSIDE_REFERENCE_MONTH",
        ),
    ]

    anomaly_conditions = [
        (
            F.col("trip_duration_minutes") > 360,
            "TRIP_DURATION_ABOVE_6_HOURS",
        ),
        (
            F.col("trip_distance") > 100,
            "TRIP_DISTANCE_ABOVE_100_MILES",
        ),
        (
            F.col("trip_distance") <= 0,
            "NON_POSITIVE_TRIP_DISTANCE",
        ),
        (
            F.col("fare_amount") <= 0,
            "NON_POSITIVE_FARE",
        ),
        (
            F.col("payment_type") == 0,
            "PAYMENT_TYPE_ZERO",
        ),
        (
            F.col("payment_type").isNull(),
            "PAYMENT_TYPE_NULL",
        ),
        (
            ~F.col("payment_type").isin(0, 1, 2, 3, 4, 5, 6)
            & F.col("payment_type").isNotNull(),
            "PAYMENT_TYPE_OUTSIDE_REFERENCE",
        ),
    ]

    rejection_flags = [
        F.when(condition, F.lit(reason))
        for condition, reason in rejection_conditions
    ]

    anomaly_flags = [
        F.when(condition, F.lit(reason))
        for condition, reason in anomaly_conditions
    ]

    df = (
        df
        .withColumn(
            "pickup_date",
            F.to_date("tpep_pickup_datetime"),
        )
        .withColumn(
            "pickup_year_month",
            F.date_format("tpep_pickup_datetime", "yyyyMM"),
        )
        .withColumn(
            "rejection_reasons",
            F.concat_ws("|", *rejection_flags),
        )
        .withColumn(
            "invalid_reason",
            F.col("rejection_reasons"),
        )
        .withColumn(
            "is_valid_trip",
            F.col("rejection_reasons") == "",
        )
        .withColumn(
            "quality_flags",
            F.concat_ws("|", *anomaly_flags),
        )
        .withColumn(
            "reference_month",
            F.lit(reference_month),
        )
        .withColumn(
            "source_file",
            F.lit(source_file),
        )
        .withColumn(
            "is_valid_revenue_payment",
            F.coalesce(
                F.col("payment_type").isin(1, 2),
                F.lit(False),
            ),
        )
        .withColumn(
            "quality_status",
            F.when(
                F.col("rejection_reasons") != "",
                "QUARANTINE",
            ).when(
                F.col("quality_flags") != "",
                "REVIEW",
            ).otherwise("APPROVED"),
        )
    )

    return df


def process_file(
    spark: SparkSession,
    file_path: Path,
) -> None:

    reference_month = extract_reference_month(file_path)

    logger.info("Iniciando processamento: %s", file_path.name)

    df = spark.read.parquet(str(file_path))

    df = standardize_types(df)

    df = add_quality_classification(
        df=df,
        reference_month=reference_month,
        source_file=file_path.name,
    )

    quarantine_df = df.filter(
        F.col("rejection_reasons") != ""
    )

    silver_df = df.filter(
        F.col("rejection_reasons") == ""
    )

    silver_path = SILVER_DIR / f"reference_month={reference_month}"
    quarantine_path = (
        QUARANTINE_DIR / f"reference_month={reference_month}"
    )

    silver_df.write.mode("overwrite").parquet(str(silver_path))

    quarantine_df.write.mode("overwrite").parquet(
        str(quarantine_path)
    )

    total = df.count()
    approved = silver_df.count()
    quarantined = quarantine_df.count()
    review = silver_df.filter(
        F.col("quality_status") == "REVIEW"
    ).count()

    if total != approved + quarantined:
        raise RuntimeError(
            f"Falha na reconciliação de {reference_month}: "
            f"{total} != {approved} + {quarantined}"
        )

    logger.info("Auditoria de %s", reference_month)
    logger.info("Total de registros: %s", f"{total:,}")
    logger.info("Silver: %s", f"{approved:,}")
    logger.info("Quarentena: %s", f"{quarantined:,}")
    logger.info("Registros sinalizados para revisão: %s", f"{review:,}")
    logger.info("Reconciliação aprovada.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Processamento Silver dos dados NYC Yellow Taxi."
    )

    parser.add_argument(
        "--year",
        type=int,
        default=2025,
    )

    parser.add_argument(
        "--start-month",
        type=int,
        default=1,
    )

    parser.add_argument(
        "--end-month",
        type=int,
        default=6,
    )

    args = parser.parse_args()

    if not 1 <= args.start_month <= args.end_month <= 12:
        parser.error("Informe um intervalo de meses válido, entre 1 e 12.")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        for month in range(args.start_month, args.end_month + 1):
            filename = (
                f"yellow_tripdata_{args.year}-{month:02d}.parquet"
            )

            file_path = BRONZE_DIR / filename

            if not file_path.exists():
                raise FileNotFoundError(
                    f"Arquivo Bronze não encontrado: {file_path}"
                )

            process_file(spark, file_path)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

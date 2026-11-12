import argparse
import logging
import os
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "yellow_taxi"

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

CHUNK_SIZE = 1024 * 1024  # 1 MB


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def create_session() -> requests.Session:
    retry_strategy = Retry(
        total=4,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    session = requests.Session()
    session.mount("https://", adapter)

    return session


def is_valid_parquet(file_path: Path) -> bool:
    if not file_path.exists() or file_path.stat().st_size < 8:
        return False

    with file_path.open("rb") as file:
        header = file.read(4)
        file.seek(-4, os.SEEK_END)
        footer = file.read(4)

    return header == b"PAR1" and footer == b"PAR1"


def download_month(
    session: requests.Session,
    year: int,
    month: int,
) -> Path:
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"

    destination = BRONZE_DIR / filename
    temporary_file = BRONZE_DIR / f"{filename}.part"

    url = f"{BASE_URL}/{filename}"

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    if is_valid_parquet(destination):
        logger.info("Arquivo já existe e é válido: %s", filename)
        return destination

    if destination.exists():
        logger.warning("Arquivo inválido encontrado. Será substituído: %s", filename)
        destination.unlink()

    logger.info("Iniciando download: %s", filename)

    try:
        with session.get(
            url,
            stream=True,
            timeout=(15, 120),
        ) as response:

            response.raise_for_status()

            with temporary_file.open("wb") as file:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        file.write(chunk)

        if not is_valid_parquet(temporary_file):
            raise ValueError(f"Arquivo Parquet inválido: {filename}")

        os.replace(temporary_file, destination)

        size_mb = destination.stat().st_size / (1024 * 1024)

        logger.info(
            "Download concluído: %s (%.2f MB)",
            filename,
            size_mb,
        )

        return destination

    except Exception:
        temporary_file.unlink(missing_ok=True)
        logger.exception("Falha no download: %s", filename)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download incremental dos dados NYC Yellow Taxi."
    )

    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--start-month", type=int, default=1)
    parser.add_argument("--end-month", type=int, default=6)

    args = parser.parse_args()

    if not 1 <= args.start_month <= 12:
        parser.error("O mês inicial deve estar entre 1 e 12.")

    if not 1 <= args.end_month <= 12:
        parser.error("O mês final deve estar entre 1 e 12.")

    if args.start_month > args.end_month:
        parser.error("O mês inicial não pode ser maior que o mês final.")

    logger.info(
        "Iniciando ingestão: %s, meses %02d até %02d",
        args.year,
        args.start_month,
        args.end_month,
    )

    with create_session() as session:
        for month in range(args.start_month, args.end_month + 1):
            download_month(session, args.year, month)

    logger.info("Ingestão finalizada com sucesso.")


if __name__ == "__main__":
    main()

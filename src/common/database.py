import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

load_dotenv()

DATABASE_URL = URL.create(
    drivername="postgresql+psycopg",
    username=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT", "5432")),
    database=os.getenv("DB_NAME"),
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args={"connect_timeout": 5},
)


def test_connection() -> None:
    with engine.connect() as connection:
        result = connection.execute(text("SELECT current_database(), version()"))
        database, version = result.one()

        print(f"Banco conectado: {database}")
        print(f"Servidor: {version.split(',')[0]}")


if __name__ == "__main__":
    test_connection()

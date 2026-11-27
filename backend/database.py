import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text


load_dotenv(Path(__file__).resolve().parents[1] / ".env")

database_url = URL.create(
    "postgresql+psycopg",
    username=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    host="127.0.0.1",
    port=int(os.getenv("POSTGRES_PORT") or "5432"),
    database=os.environ["POSTGRES_DB"],
)
engine = create_engine(database_url, connect_args={"connect_timeout": 5})


def check_database() -> bool:
    """Return whether PostgreSQL answers SELECT 1 with 1."""
    with engine.connect() as connection:
        return connection.execute(text("SELECT 1")).scalar_one() == 1

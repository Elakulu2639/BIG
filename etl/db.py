from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .config import DatabaseConfig


def create_db_engine(config: DatabaseConfig | None = None) -> Engine:
    cfg = config or DatabaseConfig()
    engine = create_engine(cfg.sqlalchemy_url, pool_pre_ping=True)
    return engine


def ensure_database_exists(config: DatabaseConfig) -> None:
    """Create the target database if it does not exist.

    This connects to the built-in 'postgres' database with the same host/port/user
    and creates config.db if missing. Uses AUTOCOMMIT as CREATE DATABASE cannot run
    inside a transaction.
    """
    admin_url = (
        f"postgresql+psycopg2://{config.user}:{config.password}@{config.host}:{config.port}/postgres"
    )
    admin_engine = create_engine(admin_url, pool_pre_ping=True)
    with admin_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :db"), {"db": config.db}
        ).scalar()
        if not exists:
            conn.execute(text(f"CREATE DATABASE \"{config.db}\""))


@contextmanager
def engine_connect(engine: Engine) -> Iterator[None]:
    with engine.begin() as conn:
        yield conn


def healthcheck(engine: Engine) -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False

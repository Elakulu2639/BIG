import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass
class DatabaseConfig:
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    db: str = os.getenv("POSTGRES_DB", "etl_db")
    user: str = os.getenv("POSTGRES_USER", "etl_user")
    password: str = os.getenv("POSTGRES_PASSWORD", "etl_password")

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
        )


@dataclass
class EtlSettings:
    csv_path: str = os.getenv("CSV_PATH", "data/5m Sales Records.csv")
    chunk_size: int = int(os.getenv("CHUNK_SIZE", "200000"))

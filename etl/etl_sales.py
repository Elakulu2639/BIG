import math
from typing import Iterable

import pandas as pd
import structlog
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .config import EtlSettings
from .db import create_db_engine

logger = structlog.get_logger()


def extract_data(csv_path: str, chunk_size: int) -> Iterable[pd.DataFrame]:
    """EXTRACT: Read CSV file in chunks"""
    logger.info("extract_start", csv_path=csv_path, chunk_size=chunk_size)
    
    chunks = pd.read_csv(
        csv_path,
        chunksize=chunk_size,
        dtype_backend="pyarrow",
    )
    
    return chunks
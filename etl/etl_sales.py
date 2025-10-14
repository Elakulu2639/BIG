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

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """TRANSFORM: Clean, normalize, and validate data"""
    logger.info("transform_start", input_rows=len(df))
    
    # 1. Column normalization
    df = df.rename(
        columns={
            "Region": "region",
            "Country": "country",
            "Item Type": "item_type",
            "Sales Channel": "sales_channel",
            "Order Priority": "order_priority",
            "Order Date": "order_date",
            "Order ID": "order_id",
            "Ship Date": "ship_date",
            "Units Sold": "units_sold",
            "Unit Price": "unit_price",
            "Unit Cost": "unit_cost",
            "Total Revenue": "total_revenue",
            "Total Cost": "total_cost",
            "Total Profit": "total_profit",
        }
    )

    # 2. Data type conversion
    date_cols = ["order_date", "ship_date"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    numeric_cols = [
        "units_sold",
        "unit_price",
        "unit_cost",
        "total_revenue",
        "total_cost",
        "total_profit",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
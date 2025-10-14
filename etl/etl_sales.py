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

    
    # 3. Handle missing values
    # Fill missing categorical data with 'Unknown'
    categorical_cols = ["region", "country", "item_type", "sales_channel", "order_priority"]
    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown")
    
    # Fill missing numeric data with 0
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)
    
    # Fill missing dates with a default date (1900-01-01)
    for col in date_cols:
        if col in df.columns:
            df[col] = df[col].fillna(pd.Timestamp("1900-01-01").date())
    
    # Fill missing order_id with a generated ID (negative to avoid conflicts)
    if "order_id" in df.columns:
        missing_order_ids = df["order_id"].isna()
        if missing_order_ids.any():
            # Generate negative IDs for missing values
            max_existing_id = df["order_id"].max() if not df["order_id"].isna().all() else 0
            start_id = min(max_existing_id - 1, -1)
            df.loc[missing_order_ids, "order_id"] = range(start_id, start_id - missing_order_ids.sum(), -1)

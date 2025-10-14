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


# 4. Remove duplicates based on order_id (primary business key)
    initial_rows = len(df)
    df = df.drop_duplicates(subset=["order_id"], keep="first")
    duplicates_removed = initial_rows - len(df)
    
    if duplicates_removed > 0:
        logger.info("duplicates_removed", count=duplicates_removed)

    # 5. Data validation and cleaning
    # Remove rows where critical fields are still invalid
    df = df.dropna(subset=["order_id"])  # Must have order_id
    
    # Ensure units_sold is positive
    if "units_sold" in df.columns:
        df = df[df["units_sold"] >= 0]
    
    # Ensure ship_date is not before order_date
    if "order_date" in df.columns and "ship_date" in df.columns:
        invalid_dates = df["ship_date"] < df["order_date"]
        if invalid_dates.any():
            logger.info("invalid_dates_fixed", count=invalid_dates.sum())
            df.loc[invalid_dates, "ship_date"] = df.loc[invalid_dates, "order_date"]

    logger.info("transform_complete", output_rows=len(df))
    return df
def load_data(engine: Engine, df: pd.DataFrame) -> int:
    """LOAD: Insert transformed data into database"""
    if df.empty:
        return 0
    
    logger.info("load_start", rows_to_load=len(df))
    
    df.to_sql(
        name="records",
        con=engine,
        schema="sales",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=10000,
    )
    
    logger.info("load_complete", rows_loaded=len(df))
    return len(df)

def ensure_schema(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS sales"))
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS sales.records (
                id BIGSERIAL PRIMARY KEY,
                region TEXT,
                country TEXT,
                item_type TEXT,
                sales_channel TEXT,
                order_priority TEXT,
                order_date DATE,
                order_id BIGINT,
                ship_date DATE,
                units_sold INTEGER,
                unit_price NUMERIC(12,2),
                unit_cost NUMERIC(12,2),
                total_revenue NUMERIC(14,2),
                total_cost NUMERIC(14,2),
                total_profit NUMERIC(14,2),
                inserted_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        ))


def run_etl(csv_path: str | None = None, chunk_size: int | None = None) -> None:
    """Main ETL pipeline following proper E-T-L order"""
    settings = EtlSettings()
    path = csv_path or settings.csv_path
    size = chunk_size or settings.chunk_size

    # Setup database
    engine = create_db_engine()
    ensure_schema(engine)

    logger.info("etl_pipeline_start", csv_path=path, chunk_size=size)

    # Determine total rows for progress tracking
    try:
        total_rows = sum(1 for _ in open(path, "r", encoding="utf-8")) - 1
    except Exception:
        total_rows = None

    # EXTRACT: Read CSV in chunks
    chunks = extract_data(path, size)

    processed = 0
    for i, chunk in enumerate(chunks, start=1):
        # TRANSFORM: Clean and validate data
        transformed_chunk = transform_data(chunk)
        
        # LOAD: Insert into database
        inserted = load_data(engine, transformed_chunk)
        
        processed += inserted
        pct = (processed / total_rows * 100) if total_rows else None
        
        logger.info(
            "etl_chunk_complete",
            chunk_number=i,
            extracted_rows=len(chunk),
            transformed_rows=len(transformed_chunk),
            loaded_rows=inserted,
            total_processed=processed,
            progress_percent=(round(pct, 2) if pct else None),
        )

    logger.info("etl_pipeline_complete", total_rows_loaded=processed)


if name == "main":
    run_etl()
import argparse

from etl.config import DatabaseConfig
from etl.db import create_db_engine, ensure_database_exists
from etl.etl_sales import ensure_schema, run_etl


def main() -> None:
    parser = argparse.ArgumentParser(description="ETL Sales Pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("initdb", help="Create database (if missing) and schema")

    etl_parser = sub.add_parser("run", help="Run ETL from CSV -> Postgres")
    etl_parser.add_argument("--csv", dest="csv_path", default=None)
    etl_parser.add_argument("--chunk", dest="chunk_size", type=int, default=None)

    args = parser.parse_args()

    if args.command == "initdb":
        cfg = DatabaseConfig()
        ensure_database_exists(cfg)
        engine = create_db_engine(cfg)
        ensure_schema(engine)
        print("Database and schema ensured.")
    elif args.command == "run":
        run_etl(csv_path=args.csv_path, chunk_size=args.chunk_size)


if __name__ == "__main__":
    main()

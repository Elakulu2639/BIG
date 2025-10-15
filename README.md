# ETL Sales Pipeline

| Name | ID |
| ---- | -- |
| Elias Aynekulu | 1402639 |
| Haileeyesus Melisew | 1402967 |
| Desalegn Mulat | 1401145 |
| Amanuel Getachew | 1500016 |
| Habtamu Kebede | 1401334 |

This project loads the large `data/5m Sales Records.csv` into PostgreSQL using a chunked Pandas ETL.

- Data source (sample sales CSVs up to 5M rows):  
  https://excelbianalytics.com/wp/downloads-18-sample-csv-files-data-sets-for-testing-sales/

## Prerequisites
- PostgreSQL installed locally (know your host, port, user, password)
- Python 3.11+

## Configure environment
1. Copy `env.example` to `.env` and set your local credentials, e.g.:
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=etl_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
CSV_PATH=data/5m Sales Records.csv
CHUNK_SIZE=200000
2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Get the CSV
- Download the sample sales data (choose size up to 5 million rows) from:  
  https://excelbianalytics.com/wp/downloads-18-sample-csv-files-data-sets-for-testing-sales/  
- Extract and place the file at `data/5m Sales Records.csv` (or update `CSV_PATH` accordingly).

## Initialize database and schema (no Docker)
```bash
python cli.py initdb
```
- This will create the database `etl_db` if it does not exist and ensure the `sales.records` table.

## Run ETL
```bash
python cli.py run --csv "data/5m Sales Records.csv" --chunk 200000
```
- Adjust `--chunk` if you have limited memory (e.g., 100000).

## Verify
Connect with any SQL client and run:
```sql
SELECT COUNT(*) FROM sales.records;
```

## Notes
- Re-running will append rows. 
- If your path contains spaces, keep quotes: `"data/5m Sales Records.csv"`.
- `pyarrow` dtype backend is used for efficient CSV parsing.
```


Get the CSV
Download the sample sales data (choose size up to 5 million rows) from:
https://excelbianalytics.com/wp/downloads-18-sample-csv-files-data-sets-for-testing-sales/
Extract and place the file at data/5m Sales Records.csv (or update CSV_PATH accordingly).
Initialize database and schema (no Docker)
python cli.py initdb
This will create the database etl_db if it does not exist and ensure the sales.records table.
Run ETL
python cli.py run --csv "data/5m Sales Records.csv
Adjust --chunk if you have limited memory (e.g., 100000).

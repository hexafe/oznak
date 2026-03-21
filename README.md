# Oznak
v0.2

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/)
[![CI](https://github.com/hexafe/oznak/actions/workflows/ci.yml/badge.svg)](https://github.com/hexafe/oznak/actions/workflows/ci.yml)

*A modular data analysis system for loading, filtering, and processing data from multiple databases.*

## Features

## Project roadmap

Implementation phases and current status are tracked in `docs/IMPLEMENTATION_ROADMAP.md`.


- Multi-database loader (MySQL, MSSQL, more to be added)
- Columns selection for fetching
- Generic filtering system (LIKE, =, >, <, IN, etc.)
- Export to CSV/Excel
- Multi-database data aggregation
- FastAPI read API for the same fetch flow
- Database-aware query generation for MySQL and MSSQL

## Installation

```bash
git clone https://github.com/hexafe/oznak.git
cd oznak
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```


## Configuration
1. Copy `.env.example` to `.env` and fill in your database credentials.
2. Edit `config/databases.yaml` to define database connections.
3. Each configured database must declare a supported `type` such as `mysql` or `mssql`.

## Usage
### CLI usage
```bash
python -m src.main load <database1,database2,...> --select-columns "<column1>,<column2>,<column3>" --filter "<column> <operator> <value>" --out <output_file>
```

### Examples
- Fetch data for specific reference:
```bash
python -m src.main load database1,database2 --filter "RefName LIKE V123456" --out data.csv
```

- Fetch last 1000 records, ordered by date column:
```bash
python -m src.main load database1 --last 1000 --date_col ProductionDate --out recent_data.xlsx
```

- Combine multiple filters and fetch specific columns data from multiple databases:
```bash
python -m src.main load database1,database2,database3 --select-columns "Status,Priority,ProductionDate" --filter "Status = ACTIVE" --filter "Priority > 5" --filter "ProductionDate > 2025-01-01" --out filtered_data.csv
```

### Chunked export
```bash
python -m src.main load-chunked database1,database2 --filter "Status = ACTIVE" --chunk-size 10000 --pagination-column id --out data.csv
```

Notes:
- Chunked export currently supports `.csv` output only.
- The pagination column must be unique and indexed for reliable paging.
- If you use `--select-columns`, the pagination column is fetched internally when needed and omitted from the final file unless you requested it.
- SQL generation is database-aware for MySQL and MSSQL, but live validation still depends on your actual schema and driver setup.

### API
```bash
uvicorn src.api.rest:app --host 0.0.0.0 --port 8000
```

Health check:
```bash
curl http://127.0.0.1:8000/health
```

Fetch data:
```bash
curl "http://127.0.0.1:8000/fetch?databases=database1,database2&filters=Status%20%3D%20ACTIVE&last=100"
```

## More options
```bash
python -m src.main --help
```

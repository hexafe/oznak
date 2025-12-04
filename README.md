# Oznak
v0.2

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/)

*A modular data analysis system for loading, filtering, and processing data from multiple sources.*

## Features

- Multi-database loader (MySQL, MSSQL, more to be added)
- Columns selection for fetching
- Generic filtering system (LIKE, =, >, <, IN, etc.)
- Export to CSV/Excel
- Multi-database data aggregation

## Installation

```bash
git clone https://github.com/hexafe/oznak.git
cd oznak
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```


## Configuration
1. Copy .env.example to .env and fill in your database credentials:
2. Edit config/databases.yaml to define database connections (host, port, database name, table)

## Usage
### Basic usage:
```bash
python -m src.main <database1,database2,...> --select-columns "<column1>,<columns2>,<column3>" --filter "<column> <operator> <value>" --out <output_file>
```

### Examples
- Fetch data for specific reference:
```bash
python -m src.main database1,database2 --filter "RefName LIKE V123456" --out data.csv
```

- Fetch last 1000 records, ordered by date column:
```bash
python -m src.main database1 --last 1000 --date-col ProductionDate --out recent_data.xlsx
```

- Combine multiple filters and fetch specific columns data from multiple databases:
```bash
python -m src.main database1,database2,database3 --select-columns "Status,Priority,ProductionDate" --filter "Status = ACTIVE" --filter "Priority > 5" --filter "ProductionDate > 2025-01-01" --out filtered_data.csv
```

## More options and informations
```bash
python -m src.main --help
```



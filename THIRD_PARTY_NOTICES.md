# Third-Party Notices

Last reviewed: 2026-05-16

This file tracks direct runtime dependencies declared in `pyproject.toml`. It is a
release-readiness checklist, not legal advice. Refresh package metadata before
publishing a release tag, especially when dependency versions are pinned or
changed.

## Runtime Dependencies

| Package | Purpose | License metadata observed in the local release audit |
|---|---|---|
| `fastapi` | Optional API surface | MIT |
| `mysql-connector-python` | MySQL connectivity | GNU GPLv2 with FOSS License Exception |
| `openpyxl` | XLSX export support | MIT |
| `pandas` | DataFrame results and file export | BSD-3-Clause |
| `PyMySQL` | MySQL connectivity | MIT |
| `pyodbc` | MSSQL ODBC connectivity | MIT |
| `python-dotenv` | Local environment file loading in adapters/tools | BSD-3-Clause |
| `PyYAML` | YAML configuration loading | MIT |
| `SQLAlchemy` | Engine creation and SQL execution | MIT |
| `typer` | CLI framework | MIT-style project metadata; verify exact package metadata before release |
| `uvicorn` | Optional ASGI server for the API | BSD-3-Clause |

## Optional Dependencies

| Package | Purpose | Release note |
|---|---|---|
| `pyarrow` | Optional Parquet export engine | Installed only through the `parquet` extra; verify metadata before distributing a release that advertises Parquet support. |

## System Prerequisites

- MSSQL connectivity requires Microsoft ODBC Driver 17 for SQL Server or a
  compatible driver available to `pyodbc`.
- MySQL connectivity uses the configured Python driver and requires network
  access to the configured database host.
- Oznak must not ship real credentials, production exports, or live database
  connection strings in source archives, wheels, examples, or docs.

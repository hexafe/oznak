# Live Database Integration Tests

Default Oznak tests are synthetic-only. Live database tests are opt-in and are
intended for release candidates or driver/database compatibility checks.

## Start Disposable Databases

```bash
docker compose -f docker-compose.integration.yml up -d
```

The compose file starts:

- MySQL on `127.0.0.1:3307`
- SQL Server on `127.0.0.1:14333`

The tests create and replace a small `records` table with synthetic rows. They
do not need real plant data or credentials.

## Run The Tests

```bash
OZNAK_RUN_LIVE_DB_TESTS=1 python -m pytest -q -m integration
```

MSSQL tests require Microsoft ODBC Driver 17 for SQL Server on the host running
pytest. If the driver is missing, the MSSQL test is skipped even when live tests
are enabled.

Override defaults with environment variables when needed:

```bash
OZNAK_IT_MYSQL_HOST=127.0.0.1
OZNAK_IT_MYSQL_PORT=3307
OZNAK_IT_MYSQL_DATABASE=oznak_it
OZNAK_IT_MYSQL_USER=oznak
OZNAK_IT_MYSQL_PASSWORD=oznak_password

OZNAK_IT_MSSQL_HOST=127.0.0.1
OZNAK_IT_MSSQL_PORT=14333
OZNAK_IT_MSSQL_DATABASE=master
OZNAK_IT_MSSQL_USER=sa
OZNAK_IT_MSSQL_PASSWORD=Oznak_Strong_Passw0rd!
```

## Stop Disposable Databases

```bash
docker compose -f docker-compose.integration.yml down -v
```

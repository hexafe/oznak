import pyodbc


def connect_mssql(cfg, user, password):
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={cfg['host']},{cfg['port']};"
            f"DATABASE={cfg['database']};"
            f"UID={user};"
            f"PWD={password};"
        )
        connection = pyodbc.connect(conn_str)
        print(f"Connected to MSSQL database: {cfg['database']}")
        return connection
    except Exception as e:
        print(f"Error connecting to MSSQL: {e}")
        return None


import yaml
from sqlalchemy import create_engine
from src.utils.env import get_credentials
from config.settings import CONFIG_PATH


class DBManager:
    def __init__(self, config_path=CONFIG_PATH):
        with open(config_path, "r", encoding="utf-8") as f:
            self.cfg = yaml.safe_load(f)["databases"]
        self.engines = {}

    def get_engine(self, database: str):
        if database not in self.cfg:
            raise ValueError(f"Database {database} not found in configuration")

        if database in self.engines:
            return self.engines[database]

        entry = self.cfg[database]
        user, password = get_credentials(database)

        # Build connection string based on database type
        if entry["type"] == "mysql":
            # Using PyMySQL driver for compatibility with SQLAlchemy
            conn_str = f"mysql+pymysql://{user}:{password}@{entry["host"]}:{entry["port"]}/{entry["database"]}"
        elif entry["type"] == "mssql":
            # Using pyodbc driver
            conn_str = f"mssql+pyodbc://{user}:{password}@{entry['host']}:{entry['port']}/{entry['database']}?driver=ODBC+Driver+17+for+SQL+Server"
        else:
            raise ValueError(f"Unsupported DB type: {entry["type"]}")

        engine = create_engine(
            conn_str,
            echo=False, # For debugging
            pool_pre_ping=True, # Verify connection before use
            # Add other options as needed: pool_size, max_overflow, etc
        )

        self.engines[database] = engine
        return engine

    def connect(self, database: str):
        """ Probably not be used anymore after SQLAlchemy integration, RIP [*] """
        if database not in self.cfg:
            raise ValueError(f"Database {database} not found in configuration")

        entry = self.cfg[database]
        user, password = get_credentials(database)

        if entry["type"] == "mysql":
            return connect_mysql(entry, user, password)
        if entry["type"] == "mssql":
            return connect_mssql(entry, user, password)

        raise ValueError(f"Unsupported DB type: {entry['type']}")


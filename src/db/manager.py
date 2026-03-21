import yaml
from urllib.parse import quote_plus

from sqlalchemy import create_engine

from config.settings import CONFIG_PATH
from src.db.connectors.mssql_connector import connect_mssql
from src.db.connectors.mysql_connector import connect_mysql
from src.utils.env import get_credentials


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
        db_type = entry["type"]

        if db_type not in {"mysql", "mssql"}:
            raise ValueError(f"Unsupported DB type: {db_type}")

        user, password = get_credentials(database)

        if not user or not password:
            raise ValueError(f"Missing credentials for database: {database}")

        encoded_user = quote_plus(user)
        encoded_password = quote_plus(password)

        if db_type == "mysql":
            conn_str = (
                f"mysql+pymysql://{encoded_user}:{encoded_password}"
                f"@{entry['host']}:{entry['port']}/{entry['database']}"
            )
        elif db_type == "mssql":
            conn_str = (
                f"mssql+pyodbc://{encoded_user}:{encoded_password}"
                f"@{entry['host']}:{entry['port']}/{entry['database']}"
                "?driver=ODBC+Driver+17+for+SQL+Server"
            )
        else:
            raise ValueError(f"Unsupported DB type: {db_type}")

        engine = create_engine(
            conn_str,
            echo=False,
            pool_pre_ping=True,
        )

        self.engines[database] = engine
        return engine

    def get_database_type(self, database: str):
        if database not in self.cfg:
            raise ValueError(f"Database {database} not found in configuration")
        return self.cfg[database]["type"]

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

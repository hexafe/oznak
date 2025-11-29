import yaml
from src.db.connectors.mysql_connector import connect_mysql
from src.db.connectors.mssql_connector import connect_mssql
from src.utils.env import get_credentials
from config.settings import CONFIG_PATH


class DBManager:
    def __init__(self, config_path=CONFIG_PATH):
        with open(config_path, "r", encoding="utf-8") as f:
            self.cfg = yaml.safe_load(f)["databases"]


    def connect(self, database: str):
        if database not in self.cfg:
            raise ValueError(f"Database {database} not found in configuration")

        entry = self.cfg[database]
        user, password = get_credentials(database)

        if entry["type"] == "mysql":
            return connect_mysql(entry, user, password)
        if entry["type"] == "mssql":
            return connect_mssql(entry, user, password)

        raise ValueError(f"Unsupported DB type: {entry['type']}")


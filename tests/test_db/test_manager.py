import yaml
import pytest
from pathlib import Path

from src.db.manager import DBManager


def _write_cfg(tmp_path: Path, db_type: str):
    cfg = {
        "databases": {
            "db1": {
                "type": db_type,
                "host": "localhost",
                "port": 3306 if db_type == "mysql" else 1433,
                "database": "sample_db",
                "table": "sample_table",
            }
        }
    }
    cfg_path = tmp_path / "databases.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return cfg_path


def test_get_engine_builds_mysql_connection_string(tmp_path, monkeypatch):
    cfg_path = _write_cfg(tmp_path, "mysql")

    monkeypatch.setattr("src.db.manager.get_credentials", lambda _: ("user", "pass"))

    captured = {}

    def fake_create_engine(conn_str, **kwargs):
        captured["conn_str"] = conn_str
        captured["kwargs"] = kwargs
        return object()

    monkeypatch.setattr("src.db.manager.create_engine", fake_create_engine)

    manager = DBManager(config_path=cfg_path)
    manager.get_engine("db1")

    assert captured["conn_str"] == "mysql+pymysql://user:pass@localhost:3306/sample_db"
    assert captured["kwargs"]["pool_pre_ping"] is True


def test_get_engine_raises_on_unsupported_database_type(tmp_path):
    cfg_path = _write_cfg(tmp_path, "sqlite")
    manager = DBManager(config_path=cfg_path)

    with pytest.raises(ValueError, match="Unsupported DB type: sqlite"):
        manager.get_engine("db1")

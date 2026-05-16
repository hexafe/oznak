from src._legacy import warn_legacy_module
from src.cli.main import app

warn_legacy_module("src.main", "oznak.cli")


if __name__ == "__main__":
    app()

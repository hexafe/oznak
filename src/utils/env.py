import os

from dotenv import load_dotenv

from src._legacy import warn_legacy_module

warn_legacy_module("src.utils.env", "oznak.credentials.EnvironmentCredentialProvider")

load_dotenv()


def get_credentials(database_name: str):
    user = os.getenv(f"{database_name.upper()}_USER")
    password = os.getenv(f"{database_name.upper()}_PASSWORD")
    return user, password

from dotenv import load_dotenv
import os

load_dotenv()


def get_credentials(database_name: str):
    user = os.getenv(f"{database_name.upper()}_USER")
    password = os.getenv(f"{database_name.upper()}_PASSWORD")
    return user, password


from dotenv import load_dotenv
import os

load_dotenv()


def get_credentials(line_name: str):
    user = os.getenv(f"{line_name.upper()}_USER")
    password = os.getenv(f"{line_name.upper()}_PASSWORD")
    return user, password


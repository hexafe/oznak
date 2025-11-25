import mysql.connector
from mysql.connector import Error


def connect_mysql(cfg, user, password):
    try:
        connection = mysql.connector.connect(
            host=cfg["host"],
            port=cfg["port"],
            user=user,
            password=password,
            database=cfg["database"]
        )
        if connection.is_connected():
            print(f"Connected to MySQL database: {cfg['database']}")
            return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None


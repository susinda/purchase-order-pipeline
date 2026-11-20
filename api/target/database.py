import os
from pathlib import Path

import pymysql
from pymysql.constants import ER


SCHEMA = Path(__file__).with_name("schema.sql")
CHECK_SCHEMA = "SELECT 1 FROM purchase_orders LIMIT 1"


def connection_options() -> dict:
    password = os.environ.get("MYSQL_PASSWORD")
    if not password:
        raise RuntimeError("MYSQL_PASSWORD is required")

    return {
        "host": os.environ.get("MYSQL_HOST", "127.0.0.1"),
        "port": int(os.environ.get("MYSQL_PORT", "3306")),
        "user": os.environ.get("MYSQL_USER", "purchase_orders_app"),
        "password": password,
        "database": os.environ.get("MYSQL_DATABASE", "purchase_orders"),
        "charset": "utf8mb4",
        "autocommit": True,
        "connect_timeout": 5,
        "read_timeout": 5,
        "write_timeout": 5,
    }


def initialize_schema(options: dict, *, check_existing: bool = True) -> None:
    with pymysql.connect(**options) as connection, connection.cursor() as cursor:
        if check_existing:
            try:
                cursor.execute(CHECK_SCHEMA)
            except pymysql.ProgrammingError as error:
                if not error.args or error.args[0] != ER.NO_SUCH_TABLE:
                    raise
            else:
                return
        cursor.execute(SCHEMA.read_text(encoding="utf-8"))

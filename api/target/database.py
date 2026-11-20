import os


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

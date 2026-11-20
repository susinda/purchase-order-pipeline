import argparse
import os

import pymysql

from .database import connection_options, initialize_schema


def main():
    parser = argparse.ArgumentParser(description="Create the Target API table in an existing MySQL database.")
    parser.parse_args()

    for name in ("MYSQL_HOST", "MYSQL_DATABASE", "MYSQL_USER"):
        if not os.environ.get(name):
            parser.error(f"{name} is required")

    try:
        options = connection_options()
        initialize_schema(options, check_existing=False)
    except (ValueError, RuntimeError, OSError, pymysql.MySQLError) as error:
        parser.exit(1, f"Could not initialize the Target API schema: {error}\n")

    print(f"Target API schema applied to {options['database']}")


if __name__ == "__main__":
    main()

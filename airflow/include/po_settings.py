"""Shared settings loader for the purchase-order Airflow DAGs.

Resolution order:
1. Environment variable injected into the Airflow worker (Astronomer).
2. ``env.vars`` in the Astro project root.
3. Caller-provided default.

No third-party dependencies. This does NOT print credentials.
"""

import os
from pathlib import Path
import re


QUALIFIED_IDENTIFIER = r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){2}"


def kafka_ca_file():
    """Resolve the configured path, or use the CA bundled in the Astro image."""
    project_dir = Path(__file__).resolve().parents[1]
    ca_file = project_dir / get_setting("KAFKA_SSL_CAFILE", "aiven_ca.pem")
    if not ca_file.is_file():
        raise ValueError(f"Kafka CA certificate not found at {ca_file}")
    return str(ca_file)


def _env_file_path():
    """Locate ``env.vars``; local Astro is usually rooted beside ``include/``."""
    module_path = Path(__file__).resolve()
    candidates = [
        module_path.parents[1] / "env.vars",  # airflow/include/po_settings.py -> airflow/env.vars
        Path.cwd() / "env.vars",
    ]
    airflow_home = os.environ.get("AIRFLOW_HOME")
    if airflow_home:
        candidates.append(Path(airflow_home) / "env.vars")
    # Handles installations where include/ is nested a level below the project.
    candidates.append(module_path.parents[2] / "env.vars")
    for path in candidates:
        if path.is_file():
            return path
    return None


def _read_env_file(name):
    file_path = _env_file_path()
    if file_path is None:
        return None
    with file_path.open("r", encoding="utf-8-sig") as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].lstrip()
            key, sep, value = line.partition("=")
            if not sep or key.strip() != name:
                continue
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]
            return value
    return None


def get_setting(name, default=None):
    """Prefer a nonempty process environment setting, then root env.vars."""
    value = os.environ.get(name)
    if value is not None and value.strip():
        return value
    value = _read_env_file(name)
    if value is not None and value.strip():
        return value
    return default


def required_setting(name):
    """Like get_setting but raise a clear error if a required key is absent."""
    value = get_setting(name)
    if value is None or not str(value).strip():
        raise ValueError(
            f"Missing configuration: {name}. Set it in Astro deployment environment "
            "or in env.vars at the Astro project root."
        )
    return value


def snowflake_raw_table():
    """Return the configured fully qualified RAW table name."""
    table = get_setting("SNOWFLAKE_RAW_TABLE", "PURCHASE_ORDER_DB.RAW.PURCHASE_ORDER_EVENTS")
    if not re.fullmatch(QUALIFIED_IDENTIFIER, table):
        raise ValueError("SNOWFLAKE_RAW_TABLE must be DATABASE.SCHEMA.TABLE")
    return table

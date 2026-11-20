from configparser import ConfigParser
import os
from pathlib import Path
import re


PROPERTIES = Path(__file__).with_name("application.properties")
ENV_REFERENCE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def application_properties(path=None):
    path = PROPERTIES if path is None else path
    parser = ConfigParser(interpolation=None)
    parser.read_string("[application]\n" + path.read_text(encoding="utf-8"), source=str(path))
    return parser["application"]


def required_setting(values, name):
    value = values.get(name, "")
    if not value.strip():
        raise ValueError(f"Set {name} before running this command")
    return value


def application_setting(name, env=None, *, override=None, properties=None):
    env = os.environ if env is None else env
    if override and env.get(override, "").strip():
        return env[override]
    properties = application_properties() if properties is None else properties
    value = required_setting(properties, name)
    reference = ENV_REFERENCE.fullmatch(value)
    return required_setting(env, reference[1]) if reference else value


def api_credentials(service, env=None):
    return (
        application_setting(f"{service}.api.username", env),
        application_setting(f"{service}.api.password", env),
    )

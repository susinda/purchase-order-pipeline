import ssl

from .settings import application_setting, required_setting


def kafka_topic(env):
    return application_setting("kafka.topic", env, override="KAFKA_TOPIC").strip()


def kafka_group_id(env, role):
    prefix = "CONSUMER" if role == "inspection" else role.upper()
    return application_setting(f"kafka.{role}.group", env, override=f"KAFKA_{prefix}_GROUP_ID").strip()


def connection_options(env, *, role=None):
    protocol = application_setting("kafka.security.protocol", env, override="KAFKA_SECURITY_PROTOCOL")
    options = {
        "bootstrap_servers": application_setting("kafka.bootstrap.servers", env),
        "security_protocol": protocol,
    }
    if protocol.startswith("SASL_"):
        username_key = "KAFKA_SASL_USERNAME"
        password_property = "kafka.sasl.password"
        if role is not None:
            prefix = f"KAFKA_{role.upper()}_SASL"
            if f"{prefix}_USERNAME" in env or f"{prefix}_PASSWORD" in env:
                username_key = f"{prefix}_USERNAME"
                password_property = f"kafka.{role}.password"
        username = env.get(username_key, "").strip()
        if not username and role is not None:
            username = application_setting(f"kafka.{role}.username", env)
        options.update(
            sasl_mechanism=application_setting("kafka.sasl.mechanism", env, override="KAFKA_SASL_MECHANISM"),
            sasl_plain_username=username or required_setting(env, username_key),
            sasl_plain_password=application_setting(password_property, env),
        )
    if protocol in {"SSL", "SASL_SSL"} and env.get("KAFKA_SSL_CAFILE"):
        options["ssl_cafile"] = env["KAFKA_SSL_CAFILE"]
    elif protocol in {"SSL", "SASL_SSL"} and env.get("KAFKA_CA_PEM"):
        options["ssl_context"] = ssl.create_default_context(cadata=env["KAFKA_CA_PEM"])
    return options

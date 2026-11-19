def required_setting(env, name):
    value = env.get(name, "")
    if not value.strip():
        raise ValueError(f"Set {name} before running a Kafka command")
    return value


def connection_options(env):
    protocol = env.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    options = {
        "bootstrap_servers": required_setting(env, "KAFKA_BOOTSTRAP_SERVERS"),
        "security_protocol": protocol,
    }
    if protocol.startswith("SASL_"):
        options.update(
            sasl_mechanism=env.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
            sasl_plain_username=required_setting(env, "KAFKA_SASL_USERNAME"),
            sasl_plain_password=required_setting(env, "KAFKA_SASL_PASSWORD"),
        )
    if protocol in {"SSL", "SASL_SSL"} and env.get("KAFKA_SSL_CAFILE"):
        options["ssl_cafile"] = env["KAFKA_SSL_CAFILE"]
    return options

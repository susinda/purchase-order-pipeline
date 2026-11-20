import argparse
from configparser import Error as PropertiesError
import json
from pathlib import Path
import re
from urllib.error import HTTPError
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen

from dags.po_pipeline.settings import PROPERTIES, application_properties, application_setting, required_setting


API_BASE = "https://api.aiven.io/v1"
DESTINATIONS = ("backend", "analytics")


def read_resources(properties):
    topic = required_setting(properties, "kafka.topic")
    dlts = {role: required_setting(properties, f"kafka.{role}.dlt") for role in DESTINATIONS}
    users = {
        role: required_setting(properties, f"kafka.{role}.username")
        for role in ("producer", *DESTINATIONS)
    }
    groups = {role: required_setting(properties, f"kafka.{role}.group") for role in DESTINATIONS}
    topics = (topic, *dlts.values())
    if len(set(topics)) != len(topics) or any(
        name in {".", ".."} or not re.fullmatch(r"[A-Za-z0-9._-]{1,249}", name) for name in topics
    ):
        raise ValueError("Configure three distinct, literal Kafka topic names")
    for names in (users.values(), groups.values()):
        if len(set(names)) != len(names) or any(re.search(r"[\s*?]", name) for name in names):
            raise ValueError("Configure distinct usernames and groups without whitespace or wildcards")

    topic_config = {
        "partitions": properties.getint("kafka.topic.partitions"),
        "replication": properties.getint("kafka.topic.replication"),
        "retention_hours": properties.getint("kafka.topic.retention.hours"),
        "min_insync_replicas": 1,
        "retention_bytes": -1,
        "cleanup_policy": "delete",
    }
    if any((topic_config[key] or 0) < 1 for key in ("partitions", "replication", "retention_hours")):
        raise ValueError("Set positive integers for topic partitions, replication, and retention hours")
    grants = [(users["producer"], "Topic", topic, "Write")]
    for role in DESTINATIONS:
        grants.extend([
            (users[role], "Topic", topic, "Read"),
            (users[role], "Topic", dlts[role], "Write"),
            (users[role], "Group", groups[role], "Read"),
        ])
    return topics, topic_config, users, groups, grants


def request(service_url, token, path, *, payload=None):
    headers = {"Accept": "application/json"}
    data = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload).encode("utf-8")
    req = Request(f"{service_url}{path}", data=data, headers=headers)
    req.add_unredirected_header("Authorization", f"Bearer {token}")
    with urlopen(req, timeout=20) as response:
        result = json.load(response)
    if not isinstance(result, dict) or result.get("error"):
        raise ValueError(f"Aiven returned an unexpected response for {req.get_method()} {path}")
    return result


def ensure_resource(api, resource, name, payload):
    collection = f"/{resource}"
    path = f"{collection}/{quote(name, safe='')}"
    try:
        api(path)
    except HTTPError as error:
        if error.code != 404:
            raise
        error.close()
    else:
        print(f"{resource.capitalize()} already exists: {name}")
        return

    try:
        api(collection, payload=payload)
    except HTTPError as error:
        if error.code != 409:
            raise
        error.close()
        api(path)
        print(f"{resource.capitalize()} already exists: {name}")
    else:
        print(f"Created {resource}: {name}")


def ensure_acls(api, grants):
    path = "/kafka/acl"
    existing = api(path).get("kafka_acl", [])
    for username, resource_type, resource_name, operation in grants:
        rule = {
            "principal": f"User:{username}",
            "host": "*",
            "resource_type": resource_type,
            "resource_name": resource_name,
            "pattern_type": "LITERAL",
            "operation": operation,
            "permission_type": "ALLOW",
        }
        description = f"{rule['principal']} {operation} on {resource_type} {resource_name}"
        if any(all(acl.get(key) == value for key, value in rule.items()) for acl in existing):
            print(f"ACL already exists: {description}")
        else:
            api(path, payload=rule)
            print(f"Granted {description}")


def main():
    parser = argparse.ArgumentParser(description="Provision Kafka topics, service users and ACLs on Aiven.")
    parser.add_argument("--properties", type=Path, default=PROPERTIES, help="Kafka application properties file")
    args = parser.parse_args()
    try:
        properties = application_properties(args.properties)
        topics, topic_config, users, groups, grants = read_resources(properties)
        project = quote(application_setting("aiven.project", properties=properties), safe="")
        service = quote(application_setting("aiven.service.name", properties=properties), safe="")
        token = application_setting("aiven.api.token", properties=properties)
        service_url = f"{API_BASE}/project/{project}/service/{service}"

        def api(path, *, payload=None):
            return request(service_url, token, path, payload=payload)

        for topic in topics:
            ensure_resource(api, "topic", topic, {**topic_config, "topic_name": topic})
        for username in users.values():
            ensure_resource(api, "user", username, {"username": username})
        ensure_acls(api, grants)
    except HTTPError as error:
        message = ""
        with error:
            try:
                path = urlsplit(error.url).path
                if not path.endswith("/user") and "/user/" not in path:
                    response = json.loads(error.read(8192))
                    message = response.get("message", "") if isinstance(response, dict) else ""
            except (ValueError, OSError):
                message = ""
        detail = " ".join(str(message).replace(token, "[redacted]").split())
        parser.exit(1, f"Aiven provisioning failed with HTTP {error.code} at {error.url}: {detail or error.reason}\n")
    except (OSError, ValueError, PropertiesError) as error:
        parser.exit(1, f"Aiven provisioning failed: {error}\n")

    print(f"Consumer group access is ready for {', '.join(groups.values())}.")
    print("Retrieve the service user credentials from Aiven and configure them in Astro.")


if __name__ == "__main__":
    main()

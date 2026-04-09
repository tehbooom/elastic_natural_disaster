"""
Elastic Natural Disaster Response - Setup Script
"""

import json
import os
import time
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv
import yaml
from typing import Any, Dict

load_dotenv()

ES_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
KB_URL = os.getenv("KIBANA_URL", "http://localhost:5601")

_api_key = os.getenv("ELASTICSEARCH_API_KEY", "")
if _api_key:
    ES_AUTH = None
    KB_AUTH = None
    ES_HEADERS = {
        "Content-Type": "application/json",
        "Authorization": f"ApiKey {_api_key}",
    }
    KB_HEADERS = {
        "kbn-xsrf": "true",
        "Content-Type": "application/json",
        "Authorization": f"ApiKey {_api_key}",
    }
else:
    ES_AUTH = (
        os.getenv("ELASTICSEARCH_USERNAME", "elastic"),
        os.getenv("ELASTICSEARCH_PASSWORD", "password"),
    )
    KB_AUTH = ES_AUTH
    ES_HEADERS = {"Content-Type": "application/json"}
    KB_HEADERS = {"kbn-xsrf": "true", "Content-Type": "application/json"}

ASSETS = os.path.join(os.path.dirname(__file__), "assets")


def _str_representer(dumper, data):
    """Custom representer for strings - use literal block style for multi-line."""
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


# Create custom dumper that preserves multi-line strings
class _LiteralDumper(yaml.SafeDumper):
    pass


_LiteralDumper.add_representer(str, _str_representer)


def wait_for_elasticsearch(timeout=300):
    print("Waiting for Elasticsearch...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{ES_URL}/", auth=ES_AUTH, timeout=5)
            if r.status_code == 200:
                return r.json().get("version", {}).get("number")
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(5)
    raise RuntimeError("Elasticsearch did not become ready in time")


def wait_for_kibana(timeout=300):
    print("Waiting for Kibana...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{KB_URL}/api/status", auth=KB_AUTH, timeout=5)
            if r.status_code == 200:
                return r.json().get("version", {}).get("number")
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(5)
    raise RuntimeError("Kibana did not become ready in time")


def create_index_template():
    print("\nCreating index template...")
    with open(os.path.join(ASSETS, "index_template.json")) as f:
        template = json.load(f)

    r = requests.put(
        f"{ES_URL}/_index_template/logs-gdacs.events",
        auth=ES_AUTH,
        headers=ES_HEADERS,
        json=template,
    )
    if r.status_code in (200, 201):
        print("  Index template created")
    else:
        print(f"  Warning: {r.status_code} - {r.text[:200]}")


def create_data_stream():
    print("\nCreating data stream...")
    r = requests.put(
        f"{ES_URL}/_data_stream/logs-gdacs.events-default",
        auth=ES_AUTH,
        headers=ES_HEADERS,
    )
    if r.status_code in (200, 201):
        print("  Data stream created")
    elif r.status_code == 400 and "already exists" in r.text:
        print("  Data stream already exists")
    else:
        print(f"  Warning: {r.status_code} - {r.text[:200]}")


def index_mitra_data():
    print("\nIndexing MITRA facilities data...")

    # Delete existing index so mapping is clean
    requests.delete(f"{ES_URL}/mitra-facilities", auth=ES_AUTH)

    # Create index with geo_shape mapping before bulk load
    requests.put(
        f"{ES_URL}/mitra-facilities",
        auth=ES_AUTH,
        headers=ES_HEADERS,
        json={
            "mappings": {"properties": {
                "entity_geo_location": {"type": "geo_shape"},
                "entity_geo_point": {"type": "geo_point"}
            }}
        },
    )

    with open(os.path.join(ASSETS, "index-mitra_facilities.ndjson"), "rb") as f:
        data = f.read()

    r = requests.post(
        f"{ES_URL}/_bulk",
        auth=ES_AUTH,
        headers={**ES_HEADERS, "Content-Type": "application/x-ndjson"},
        data=data,
    )
    resp = r.json()
    errors = [
        list(i.values())[0]
        for i in resp.get("items", [])
        if list(i.values())[0].get("error")
    ]
    print(
        f"  Bulk indexed {len(resp.get('items', []))} docs, {len(errors)} errors")
    for e in errors[:3]:
        print(f"    Error: {e['error']}")


def add_enrich_policy():
    print("\nAdding enrich policy...")
    with open(os.path.join(ASSETS, "enrich_policy.json")) as f:
        policy = json.load(f)

    # Pipeline must be deleted first — it holds a reference to the enrich policy
    requests.delete(
        f"{ES_URL}/_ingest/pipeline/logs-gdacs.events%40custom", auth=ES_AUTH
    )
    requests.delete(f"{ES_URL}/_enrich/policy/facilities-geo", auth=ES_AUTH)

    r = requests.put(
        f"{ES_URL}/_enrich/policy/facilities-geo",
        auth=ES_AUTH,
        headers=ES_HEADERS,
        json=policy,
    )
    if r.status_code in (200, 201):
        print("  Enrich policy created")
    else:
        print(f"  Warning: {r.status_code} - {r.text[:200]}")


def execute_enrich_policy():
    print("\nExecuting enrich policy...")
    r = requests.post(
        f"{ES_URL}/_enrich/policy/facilities-geo/_execute",
        auth=ES_AUTH,
        headers=ES_HEADERS,
    )
    if r.status_code in (200, 201):
        print("  Enrich index built successfully")
    else:
        print(f"  Warning: {r.status_code} - {r.text[:200]}")


def create_custom_pipeline():
    print("\nCreating ingest pipeline...")
    with open(os.path.join(ASSETS, "ingest_pipeline.json")) as f:
        pipeline = json.load(f)

    r = requests.put(
        f"{ES_URL}/_ingest/pipeline/logs-gdacs.events%40custom",
        auth=ES_AUTH,
        headers=ES_HEADERS,
        json=pipeline,
    )
    if r.status_code in (200, 201):
        print("  Custom ingest pipeline created")
    else:
        print(f"  Warning: {r.status_code} - {r.text[:200]}")


def create_detection_rule(workflow_id):
    print("\nCreating detection rule...")
    with open(os.path.join(ASSETS, "detection_rule.json")) as f:
        content = f.read()

    content = content.replace("REPLACE_WORKFLOW_ID", workflow_id)
    rule = json.loads(content)

    rule.pop("revision", None)

    r = requests.post(
        f"{KB_URL}/api/detection_engine/rules",
        auth=KB_AUTH,
        headers=KB_HEADERS,
        json=rule,
    )
    if r.status_code in (200, 201):
        rule_id = r.json().get("id")
        print(f"  Detection rule created (id={rule_id})")
        return rule_id
    elif r.status_code == 409:
        print("  Detection rule already exists")
        # Fetch the existing rule id
        r2 = requests.get(
            f"{KB_URL}/api/detection_engine/rules?rule_id={rule.get('rule_id', '')}",
            auth=KB_AUTH,
            headers=KB_HEADERS,
        )
        return r2.json().get("id") if r2.status_code == 200 else None
    else:
        print(f"  Warning: {r.status_code} - {r.text[:300]}")
        return None


def enable_detection_rule(rule_id):
    print("\nEnabling detection rule...")
    if not rule_id:
        print("  Skipped (no rule id)")
        return

    r = requests.patch(
        f"{KB_URL}/api/detection_engine/rules",
        auth=KB_AUTH,
        headers=KB_HEADERS,
        json={"id": rule_id, "enabled": True},
    )
    if r.status_code in (200, 201):
        print("  Detection rule enabled")
    else:
        print(f"  Warning: {r.status_code} - {r.text[:200]}")


def index_hurricane():
    print("\nIndexing hurricane Norfolk event...")
    with open(os.path.join(ASSETS, "index-hurricane_norfolk.json")) as f:
        doc = json.load(f)

    doc["properties"]["fromdate"] = datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    es_doc = {"message": json.dumps(doc)}

    r = requests.post(
        f"{ES_URL}/logs-gdacs.events-default/_doc",
        auth=ES_AUTH,
        headers=ES_HEADERS,
        json=es_doc,
    )
    if r.status_code in (200, 201):
        doc_id = r.json().get("_id")
        print(f"  Hurricane event indexed (id={doc_id})")
    else:
        print(f"  Warning: {r.status_code} - {r.text[:300]}")


def search_workflows(query: str = "", page: int = 1, limit: int = 20) -> Dict:
    r = requests.post(
        f"{KB_URL}/api/workflows/search",
        auth=KB_AUTH,
        headers=({**KB_HEADERS, "X-Elastic-Internal-Origin": "Kibana"}),
        json={"query": query, "page": page, "limit": limit},
    )
    r.raise_for_status()
    return r.json()


def update_workflow(workflow_id: str, yaml: str) -> Dict:
    payload = {"yaml": yaml}
    r = requests.put(
        f"{KB_URL}/api/workflows/{workflow_id}",
        auth=KB_AUTH,
        headers=({**KB_HEADERS, "X-Elastic-Internal-Origin": "Kibana"}),
        json=payload,
    )
    r.raise_for_status()
    return r.json()


def create_workflow(yaml: str) -> Dict:
    payload = {"yaml": yaml}
    r = requests.post(
        f"{KB_URL}/api/workflows",
        auth=KB_AUTH,
        headers=({**KB_HEADERS, "X-Elastic-Internal-Origin": "Kibana"}),
        json=payload,
    )
    r.raise_for_status()
    return r.json()


def get_tool(tool_id: str) -> Dict:
    r = requests.get(
        f"{KB_URL}/api/agent_builder/tools/{tool_id}",
        auth=KB_AUTH,
        headers=KB_HEADERS,
    )
    r.raise_for_status()

    return r.json()


def create_tool(tool_data: Dict[str, Any]) -> Dict:
    r = requests.post(
        f"{KB_URL}/api/agent_builder/tools",
        auth=KB_AUTH,
        headers=KB_HEADERS,
        json=tool_data,
    )
    r.raise_for_status()
    return r.json()


def update_tool(tool_id: str, tool_data: Dict[str, Any]) -> Dict:
    r = requests.put(
        f"{KB_URL}/api/agent_builder/tools/{tool_id}",
        auth=KB_AUTH,
        headers=KB_HEADERS,
        json=tool_data,
    )
    r.raise_for_status()
    return r.json()


def get_agent(agent_id: str) -> Dict:
    r = requests.get(
        f"{KB_URL}/api/agent_builder/agents/{agent_id}",
        auth=KB_AUTH,
        headers=KB_HEADERS,
    )
    r.raise_for_status()
    return r.json()


def create_agent(agent_data: Dict[str, Any]) -> Dict:
    r = requests.post(
        f"{KB_URL}/api/agent_builder/agents",
        auth=KB_AUTH,
        headers=KB_HEADERS,
        json=agent_data,
    )
    r.raise_for_status()
    return r.json()


def update_agent(agent_id: str, agent_data: Dict[str, Any]) -> Dict:
    r = requests.put(
        f"{KB_URL}/api/agent_builder/agents/{agent_id}",
        auth=KB_AUTH,
        headers=KB_HEADERS,
        json=agent_data,
    )
    r.raise_for_status()
    return r.json()


def upload_tool(file_name, workflow_id=None):
    with open(os.path.join(ASSETS, file_name), "r") as f:
        tool_data = yaml.safe_load(f)

    tool_id = tool_data.get("id")
    tool_type = tool_data.get("type")
    configuration = tool_data.get("configuration", {}).copy()
    if tool_type == "workflow":
        configuration["workflow_id"] = workflow_id
    try:
        get_tool(tool_id)
        update_data = {
            "description": tool_data.get("description"),
            "tags": tool_data.get("tags", []),
            "configuration": configuration,
        }
        update_tool(tool_id, update_data)
    except Exception as e:
        # Check if it's a 404 (not found) or other error
        if hasattr(e, "response") and hasattr(e.response, "status_code"):
            if e.response.status_code == 404:
                create_data = {
                    "id": tool_id,
                    "type": tool_data.get("type"),
                    "description": tool_data.get("description"),
                    "tags": tool_data.get("tags", []),
                    "configuration": configuration,
                }
                create_tool(create_data)
                print(f"   ✓ Created: {tool_id}")
            else:
                raise
        else:
            # Assume tool doesn't exist if we can't determine the error
            create_data = {
                "id": tool_id,
                "type": tool_data.get("type"),
                "description": tool_data.get("description"),
                "tags": tool_data.get("tags", []),
                "configuration": configuration,
            }
            create_tool(create_data)


def upload_workflow(file_name):
    with open(os.path.join(ASSETS, file_name), "r") as f:
        workflow_data = yaml.safe_load(f)

    workflow_name = workflow_data.get("name")
    workflow_yaml_string = yaml.dump(
        workflow_data,
        Dumper=_LiteralDumper,
        default_flow_style=False,
        sort_keys=False,
    )

    search_results = search_workflows(query=workflow_name, limit=100)
    existing_workflow = None

    for result in search_results.get("results", []):
        if result.get("name") == workflow_name:
            existing_workflow = result
            break

    if existing_workflow:
        workflow_id = existing_workflow.get("id")
        update_workflow(workflow_id, workflow_yaml_string)
        return workflow_id
    else:
        created_workflow = create_workflow(workflow_yaml_string)
        workflow_id = created_workflow.get("id")
        return workflow_id


def upload_agent(file_name):
    with open(os.path.join(ASSETS, file_name), "r") as f:
        agent_data = yaml.safe_load(f)

    agent_id = agent_data.get("id")

    try:
        get_agent(agent_id)
        update_data = {
            "name": agent_data.get("name"),
            "description": agent_data.get("description"),
            "labels": agent_data.get("labels", []),
            "avatar_color": agent_data.get("avatar_color"),
            "avatar_symbol": agent_data.get("avatar_symbol"),
            "configuration": agent_data.get("configuration", {}),
        }
        update_agent(agent_id, update_data)

    except Exception as e:
        # Check if it's a 404 (not found) or other error
        if hasattr(e, "response") and hasattr(e.response, "status_code"):
            if e.response.status_code == 404:
                # Agent doesn't exist, create it
                create_data = {
                    "id": agent_id,
                    "name": agent_data.get("name"),
                    "description": agent_data.get("description"),
                    "labels": agent_data.get("labels", []),
                    "avatar_color": agent_data.get("avatar_color"),
                    "avatar_symbol": agent_data.get("avatar_symbol"),
                    "configuration": agent_data.get("configuration", {}),
                }
                create_agent(create_data)
            else:
                raise
        else:
            # Assume agent doesn't exist if we can't determine the error
            create_data = {
                "id": agent_id,
                "name": agent_data.get("name"),
                "description": agent_data.get("description"),
                "labels": agent_data.get("labels", []),
                "avatar_color": agent_data.get("avatar_color"),
                "avatar_symbol": agent_data.get("avatar_symbol"),
                "configuration": agent_data.get("configuration", {}),
            }
            create_agent(create_data)
    except Exception as e:
        print(f"   ✗ Failed to import {file_name}: {e}")


def enable_workflows_ui():
    try:
        r = requests.post(
            f"{KB_URL}/internal/kibana/settings",
            auth=KB_AUTH,
            headers=({**KB_HEADERS, "X-Elastic-Internal-Origin": "Kibana"}),
            json={"changes": {"workflows:ui:enabled": True}},
        )
        r.raise_for_status()
    except Exception as e:
        print(f" ⚠ Could not enable workflows UI: {e}")


def main():
    print("=== Elastic Natural Disaster Response Setup ===\n")

    wait_for_elasticsearch()
    wait_for_kibana()

    enable_workflows_ui()
    index_mitra_data()
    time.sleep(10)
    add_enrich_policy()
    execute_enrich_policy()
    time.sleep(10)
    create_custom_pipeline()
    create_index_template()
    create_data_stream()
    email_workflow_id = upload_workflow("workflow-send_email.yml")
    nearest_workflow_id = upload_workflow("workflow-nearest_mitra.yml")
    agent_workflow_id = upload_workflow("workflow-response_agent.yml")
    upload_tool("tool-mitra.nearest_facility.yml", nearest_workflow_id)
    upload_tool("tool-mitra.send_email.yml", email_workflow_id)
    upload_agent("agent-mitra.response.yml")
    rule_id = create_detection_rule(agent_workflow_id)
    index_hurricane()
    enable_detection_rule(rule_id)

    print("\n=== Setup complete ===")


if __name__ == "__main__":
    main()

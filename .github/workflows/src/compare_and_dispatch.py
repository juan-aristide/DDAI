#!/usr/bin/env python3
"""
Fivetran webhook handler: checks tenant existence and conditionally triggers
Auto_Tenant_models_generator for new tenants.
"""

import json
import os
import sys
import requests


def find_project_root(start_path):
    """Find project root by walking up to .git directory."""
    current_path = os.path.abspath(start_path)
    while True:
        if os.path.exists(os.path.join(current_path, ".git")):
            return current_path
        parent_path = os.path.dirname(current_path)
        if parent_path == current_path:
            return None
        current_path = parent_path


def main():
    payload_str = os.environ.get("PAYLOAD")
    if not payload_str:
        print("PAYLOAD env var not set")
        sys.exit(1)

    try:
        payload = json.loads(payload_str)
    except json.JSONDecodeError as e:
        print(f"Invalid PAYLOAD JSON: {e}")
        sys.exit(1)

    connector_name = payload.get("connector_name")
    destination_group_id = payload.get("destination_group_id")

    if not connector_name:
        print("connector_name missing from payload")
        sys.exit(1)

    # Filter: sync_end + SUCCESSFUL
    if payload.get("event") != "sync_end":
        print(f"Event is '{payload.get('event')}', not sync_end. Exiting.")
        sys.exit(0)
    data = payload.get("data") or {}
    if data.get("status") != "SUCCESSFUL":
        print(f"Status is '{data.get('status')}', not SUCCESSFUL. Exiting.")
        sys.exit(0)

    # Extract tenantId from connector_name (format: tenantId_platform)
    tenant_id = connector_name.split("_")[0] if "_" in connector_name else connector_name
    if not tenant_id:
        print("Could not extract tenantId from connector_name")
        sys.exit(1)

    if tenant_id.lower() == "fivetran":
        print("Ignoring connector with tenantId 'fivetran'. Exiting.")
        sys.exit(0)

    # Tenant existence check
    start_path = os.path.dirname(os.path.abspath(__file__))
    project_root = find_project_root(start_path)
    if not project_root:
        print("Project root not found")
        sys.exit(1)

    customers_dir = os.path.join(project_root, "models", "customers")
    cdm_name = f"cdm_{tenant_id}"
    tenant_exists = False

    if os.path.isdir(customers_dir):
        for root, dirs, _ in os.walk(customers_dir):
            if cdm_name in dirs:
                tenant_exists = True
                break

    if tenant_exists:
        print(f"Tenant '{tenant_id}' already exists. Exiting.")
        sys.exit(0)

    # Resolve database from destination_group_id
    if not destination_group_id:
        print("destination_group_id missing from payload")
        sys.exit(1)

    dest_group_db_map_str = os.environ.get("DEST_GROUP_DB_MAP")
    if not dest_group_db_map_str:
        print("DEST_GROUP_DB_MAP env var not set")
        sys.exit(1)

    try:
        dest_group_db_map = json.loads(dest_group_db_map_str)
    except json.JSONDecodeError as e:
        print(f"Invalid DEST_GROUP_DB_MAP JSON: {e}")
        sys.exit(1)

    database = dest_group_db_map.get(destination_group_id)
    if not database:
        print(
            f"destination_group_id '{destination_group_id}' not found in DEST_GROUP_DB_MAP"
        )
        sys.exit(1)

    # Trigger workflow
    gh_token = os.environ.get("GH_TOKEN")
    if not gh_token:
        print("GH_TOKEN env var not set")
        sys.exit(1)

    repo = os.environ.get("GITHUB_REPOSITORY")
    if not repo:
        print("GITHUB_REPOSITORY env var not set")
        sys.exit(1)

    workflow_file = "Auto_Tenant_models_generator.yml"
    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_file}/dispatches"

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {gh_token}",
    }
    body = {
        "ref": "master",
        "inputs": {"database": database},
    }

    resp = requests.post(url, headers=headers, json=body, timeout=30)

    if not resp.ok:
        print(f"Failed to trigger workflow: {resp.status_code} {resp.text}")
        sys.exit(1)

    print(f"Triggered Auto_Tenant_models_generator for tenant='{tenant_id}' with database={database}")


if __name__ == "__main__":
    main()

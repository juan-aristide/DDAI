#!/usr/bin/env python3
"""Compare and dispatch script - processes repository_dispatch payload.
   For now outputs the received payload for testing triggers in GitHub Actions UI.
   Future: will use payload data + secrets to connect to Postgres (DDAI_DW_DEV).
"""

import json
import os


def main():
    payload_str = os.environ.get("PAYLOAD", "{}")
    print("=== Compare and Dispatch - Received Payload ===")
    try:
        payload = json.loads(payload_str)
        print(json.dumps(payload, indent=2))
    except json.JSONDecodeError:
        print("(raw - not valid JSON)")
        print(payload_str)
    print("=== End Payload ===")


if __name__ == "__main__":
    main()

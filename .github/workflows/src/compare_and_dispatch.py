#!/usr/bin/env python3
"""Compare and dispatch script - processes repository_dispatch payload.
   Connects to Postgres and saves payload to gh_audit.actions.
"""

import json
import os

import psycopg2
from psycopg2.extras import Json


def main():
    payload_str = os.environ.get("PAYLOAD", "{}")
    print("=== Compare and Dispatch - Received Payload ===")
    try:
        payload = json.loads(payload_str)
        print(json.dumps(payload, indent=2))
    except json.JSONDecodeError:
        print("(raw - not valid JSON)")
        print(payload_str)
        payload = {"raw": payload_str}
    print("=== End Payload ===")

    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        database=os.environ["POSTGRES_DATABASE"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO gh_audit.actions (payload) VALUES (%s)",
                (Json(payload),),
            )
        conn.commit()
        print("Payload saved to gh_audit.actions")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

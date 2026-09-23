from __future__ import annotations

import json
import os
import sys
from typing import Any

import requests
import sqlalchemy as sa

TB_GQL = "https://be-terraboost-v3.terraboost.com/graphql"
MONDAY_GQL = "https://api.monday.com/v2"
PRINT_STATUS_BOARD_ID = int(os.environ.get("PRINT_STATUS_BOARD_ID", "6920657806"))


def tb_login() -> str:
    email = (os.environ.get("TERRABOOST_EMAIL") or "").strip()
    password = (os.environ.get("TERRABOOST_PASSWORD") or "").strip()
    if not email or not password:
        raise RuntimeError("Terraboost credentials are not configured")
    q = """mutation Login($email: String!, $password: String!) {
      login(input: {email: $email, password: $password}) { token refreshToken }
    }"""
    r = requests.post(TB_GQL, json={"query": q, "variables": {"email": email, "password": password}}, timeout=30)
    r.raise_for_status()
    j = r.json()
    if j.get("errors"):
        raise RuntimeError("Terraboost login failed: " + "; ".join(str(e.get("message") or e) for e in j["errors"]))
    token = (((j.get("data") or {}).get("login") or {}).get("token") or "").strip()
    if not token:
        raise RuntimeError("Terraboost login returned no token")
    return token


def _tb_query(token: str, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    r = requests.post(
        TB_GQL,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"query": query, "variables": variables or {}},
        timeout=30,
    )
    # Preserve GraphQL/body errors even when the gateway returns 400.
    try:
        body = r.json()
    except Exception:
        body = {"raw": r.text[:1000]}
    return {"http_status": r.status_code, "body": body}


def inspect_tb_schema(token: str) -> dict[str, Any]:
    q = """query PackingSchema {
      workOrderType: __type(name: "WorkOrder") {
        name
        fields(includeDeprecated: true) {
          name
          args { name type { kind name ofType { kind name } } }
          type { kind name ofType { kind name ofType { kind name } } }
        }
      }
      connectionType: __type(name: "WorkOrdersConnection") {
        name
        fields(includeDeprecated: true) {
          name
          args { name type { kind name ofType { kind name } } }
          type { kind name ofType { kind name ofType { kind name } } }
        }
      }
      queryType: __type(name: "Query") {
        fields(includeDeprecated: true) {
          name
          args { name type { kind name ofType { kind name } } }
          type { kind name ofType { kind name ofType { kind name } } }
        }
      }
    }"""
    raw = _tb_query(token, q)
    body = raw.get("body") or {}
    if body.get("errors"):
        return {"http_status": raw.get("http_status"), "errors": body.get("errors")}
    data = body.get("data") or {}
    query_fields = []
    for fld in ((data.get("queryType") or {}).get("fields") or []):
        n = str(fld.get("name") or "")
        if any(term in n.lower() for term in ("work", "order", "pack", "print", "pdf", "file", "document")):
            query_fields.append(fld)
    return {
        "workOrderType": data.get("workOrderType"),
        "connectionType": data.get("connectionType"),
        "queryFields": query_fields,
    }


def inspect_recent_orange_accepted() -> dict[str, Any]:
    db_url = (os.environ.get("DATABASE_URL") or "").strip()
    if not db_url:
        return {"error": "DATABASE_URL not configured"}
    engine = sa.create_engine(db_url, pool_pre_ping=True)
    with engine.connect() as conn:
        row = conn.execute(sa.text("""
            SELECT r.wo
            FROM routes r
            LEFT JOIN contractors c ON c.id = r.contractor_id
            WHERE r.status::text = 'accepted'
              AND lower(coalesce(c.pod_color, r.payload->>'pod', r.payload->>'pod_name', '')) = 'orange'
            ORDER BY r.updated_at DESC
            LIMIT 1
        """)).mappings().first()
    return {"wo": row["wo"] if row else None}


def inspect_monday_board() -> dict[str, Any]:
    token = (os.environ.get("MONDAY_API_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("MONDAY_API_TOKEN is not configured")
    q = """query($ids:[ID!]!) {
      boards(ids:$ids) {
        id
        name
        groups { id title }
        columns { id title type }
        items_page(limit: 20) {
          items {
            id
            name
            column_values(ids:["files__1"]) { id text value }
          }
        }
      }
    }"""
    r = requests.post(
        MONDAY_GQL,
        headers={"Authorization": token, "Content-Type": "application/json"},
        json={"query": q, "variables": {"ids": [PRINT_STATUS_BOARD_ID]}},
        timeout=30,
    )
    r.raise_for_status()
    j = r.json()
    if j.get("errors"):
        raise RuntimeError("Monday API error: " + "; ".join(str(e.get("message") or e) for e in j["errors"]))
    return (j.get("data") or {}).get("boards", [None])[0] or {}


def main() -> None:
    result: dict[str, Any] = {"mode": "inspect", "board_id": PRINT_STATUS_BOARD_ID}
    try:
        token = tb_login()
        result["terraboost"] = inspect_tb_schema(token)
    except Exception as exc:
        result["terraboost_error"] = str(exc)
    try:
        result["orange_sample"] = inspect_recent_orange_accepted()
    except Exception as exc:
        result["orange_sample_error"] = str(exc)
    try:
        result["monday"] = inspect_monday_board()
    except Exception as exc:
        result["monday_error"] = str(exc)

    # Do not print credentials or auth tokens.
    # Keep one compact line so Railway logs are readable.
    print("PACKING_PROBE=" + json.dumps(result, separators=(",", ":"), default=str))


if __name__ == "__main__":
    main()

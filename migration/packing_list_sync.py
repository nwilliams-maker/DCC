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
    """Probe likely root fields without relying on GraphQL introspection."""
    candidates = [
        "workOrders", "workorders", "workOrder", "workorder",
        "orders", "order", "packingLists", "packingList",
        "workOrderPackingList", "printWorkOrder", "workOrderPrint",
        "workOrderPdf", "workOrderPDF",
    ]
    results: dict[str, Any] = {}
    for field in candidates:
        # __typename is legal for object/list object selections and is useful
        # for eliciting exact missing-argument/type errors without reading data.
        q = f"query PackingProbe {{ {field} {{ __typename }} }}"
        results[field] = _tb_query(token, q)
    # Discover WorkOrdersConnection container fields and WorkOrder fields.
    connection_probes: dict[str, Any] = {}
    for container_field in ("data", "nodes", "items", "edges"):
        q = f"query ConnectionProbe {{ workOrders {{ {container_field} {{ __typename }} }} }}"
        connection_probes[container_field] = _tb_query(token, q)

    work_order_fields: dict[str, Any] = {}
    # Lighthouse-style connections generally expose `data`; probe candidate
    # scalar/document fields one at a time so one bad field cannot mask others.
    for field in (
        "id", "name", "workOrderNumber", "workOrder", "orderNumber", "number",
        "packingList", "packingListUrl", "packingListURL", "packingSlip",
        "packingSlipUrl", "packingSlipURL", "pdf", "pdfUrl", "pdfURL",
        "file", "fileUrl", "downloadUrl", "document", "documents", "files",
    ):
        q = f"query WorkOrderFieldProbe {{ workOrders {{ nodes {{ {field} }} }} }}"
        work_order_fields[field] = _tb_query(token, q)

    return {
        "candidate_probes": results,
        "connection_probes": connection_probes,
        "work_order_fields": work_order_fields,
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
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
import os
import sys
from typing import Any

import requests

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


def inspect_tb_schema(token: str) -> dict[str, Any]:
    q = """query PackingSchemaInspection {
      __schema {
        queryType { name }
        types {
          kind
          name
          fields(includeDeprecated: true) {
            name
            args {
              name
              type { kind name ofType { kind name ofType { kind name } } }
            }
            type { kind name ofType { kind name ofType { kind name } } }
          }
        }
      }
    }"""
    r = requests.post(
        TB_GQL,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"query": q},
        timeout=40,
    )
    r.raise_for_status()
    j = r.json()
    if j.get("errors"):
        return {"errors": j["errors"]}
    schema = ((j.get("data") or {}).get("__schema") or {})
    terms = ("work", "order", "packing", "print", "pdf", "document", "file")
    matches = []
    for t in schema.get("types") or []:
        tname = str(t.get("name") or "")
        fields = t.get("fields") or []
        if any(term in tname.lower() for term in terms):
            matches.append({"type": tname, "kind": t.get("kind"), "fields": fields})
            continue
        field_hits = [f for f in fields if any(term in str(f.get("name") or "").lower() for term in terms)]
        if field_hits:
            matches.append({"type": tname, "kind": t.get("kind"), "fields": field_hits})
    return {"queryType": schema.get("queryType"), "matches": matches}


def inspect_monday_board() -> dict[str, Any]:
    token = (os.environ.get("MONDAY_API_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("MONDAY_API_TOKEN is not configured")
    q = """query($ids:[ID!]!) {
      boards(ids:$ids) {
        id
        name
        groups { id title }
        columns { id title type settings_str }
        items_page(limit: 25) {
          items { id name column_values { id text value } }
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
        result["monday"] = inspect_monday_board()
    except Exception as exc:
        result["monday_error"] = str(exc)

    # Do not print credentials or auth tokens.
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()

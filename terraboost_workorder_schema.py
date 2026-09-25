"""
terraboost_workorder_schema.py
===============================
READ-ONLY GraphQL schema introspection against manage.terraboost.com's API,
used to map the exact field names on the `WorkOrder` type and the arguments
accepted by `Query.workOrders`, so a packing-list PDF download worker can be
built correctly.

Endpoint: POST https://be-terraboost-v3.terraboost.com/graphql
Auth:     login(input: {email, password}) -> token, then
          `Authorization: Bearer <token>`

Safety contract:
- ONE auth mutation is hardcoded (`_LOGIN_MUTATION`) — used only to get a token.
- The only other GraphQL operations sent are introspection queries
  (`__type(name: ...)`). No business-data queries, no mutations, no writes.

This is a standalone script (no Streamlit dependency). Run it as:

    python terraboost_workorder_schema.py

It prints pretty-printed JSON to stdout describing:
    1. WorkOrder field names + types
    2. Query.workOrders field: its arguments + return type
    3. WorkOrderFilter input field names/types (if that type exists)
"""

from __future__ import annotations

import json
import os
import sys
import time

import requests

GQL_URL = "https://be-terraboost-v3.terraboost.com/graphql"
HTTP_TIMEOUT = 20


# -- Hardcoded auth mutation --------------------------------------------------
# This is the ONLY mutation this script ever sends. It does not write business
# data — it exchanges credentials for a session token.
_LOGIN_MUTATION = """mutation Login($email: String!, $password: String!) {
  login(input: {email: $email, password: $password}) {
    token
    refreshToken
  }
}"""


# -- Read-only introspection queries ------------------------------------------
_TYPE_FIELDS_QUERY = """query IntrospectType($name: String!) {
  __type(name: $name) {
    name
    kind
    fields {
      name
      type {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
          }
        }
      }
    }
  }
}"""

_QUERY_ROOT_QUERY = """query IntrospectQueryRoot {
  __type(name: "Query") {
    name
    kind
    fields {
      name
      args {
        name
        type {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
            }
          }
        }
      }
      type {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
          }
        }
      }
    }
  }
}"""

_INPUT_FIELDS_QUERY = """query IntrospectInputType($name: String!) {
  __type(name: $name) {
    name
    kind
    inputFields {
      name
      type {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
          }
        }
      }
    }
  }
}"""


def _login() -> str | None:
    """
    Calls the login mutation with credentials from env vars.
    Returns a bearer token string or None on failure.
    """
    email = os.environ.get("TERRABOOST_EMAIL")
    password = os.environ.get("TERRABOOST_PASSWORD")
    if not email or not password:
        print("[terraboost_workorder_schema._login] TERRABOOST_EMAIL/TERRABOOST_PASSWORD "
              "not set — cannot authenticate.", file=sys.stderr, flush=True)
        return None
    try:
        r = requests.post(
            GQL_URL,
            headers={"content-type": "application/json"},
            json={"query": _LOGIN_MUTATION, "variables": {"email": email, "password": password}},
            timeout=HTTP_TIMEOUT,
        )
        if r.status_code != 200:
            print(f"[terraboost_workorder_schema._login] login HTTP {r.status_code}",
                  file=sys.stderr, flush=True)
            return None
        body = r.json()
        if body.get("errors"):
            print(f"[terraboost_workorder_schema._login] login GraphQL errors: "
                  f"{body.get('errors')}", file=sys.stderr, flush=True)
            return None
        return ((body.get("data") or {}).get("login") or {}).get("token")
    except Exception as e:
        print(f"[terraboost_workorder_schema._login] {type(e).__name__}: {e}",
              file=sys.stderr, flush=True)
        return None


def _query(token: str, query_str: str, variables: dict | None = None) -> dict:
    """
    READ-ONLY GraphQL call. Asserts the operation is a `query` block —
    refuses anything else. This is the safety net.
    """
    stripped = query_str.lstrip()
    if not stripped.startswith("query"):
        raise RuntimeError(
            "terraboost_workorder_schema is read-only — only `query` blocks allowed. "
            f"Got: {stripped[:40]!r}"
        )
    headers = {
        "content-type": "application/json",
        "authorization": f"Bearer {token}",
    }
    try:
        r = requests.post(
            GQL_URL,
            headers=headers,
            json={"query": query_str, "variables": variables or {}},
            timeout=HTTP_TIMEOUT,
        )
        if r.status_code != 200:
            print(f"[terraboost_workorder_schema._query] HTTP {r.status_code}",
                  file=sys.stderr, flush=True)
            return {}
        body = r.json()
        if isinstance(body, dict) and body.get("errors"):
            # GraphQL errors arrive inside an HTTP 200 — surface them, but still
            # return the body so any partial `data` remains usable downstream.
            print(f"[terraboost_workorder_schema._query] GraphQL errors: "
                  f"{body.get('errors')}", file=sys.stderr, flush=True)
        return body
    except Exception as e:
        print(f"[terraboost_workorder_schema._query] {type(e).__name__}: {e}",
              file=sys.stderr, flush=True)
        return {}


def main() -> int:
    token = _login()
    if not token:
        print(json.dumps({"error": "authentication failed — check TERRABOOST_EMAIL/"
                                     "TERRABOOST_PASSWORD"}, indent=2))
        return 1

    result: dict = {
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "endpoint": GQL_URL,
    }

    # 1. WorkOrder type fields
    wo_resp = _query(token, _TYPE_FIELDS_QUERY, {"name": "WorkOrder"})
    result["WorkOrder"] = (wo_resp.get("data") or {}).get("__type")

    # 2. Query.workOrders field — arguments + return type
    query_root_resp = _query(token, _QUERY_ROOT_QUERY)
    query_type = (query_root_resp.get("data") or {}).get("__type") or {}
    work_orders_field = None
    for f in (query_type.get("fields") or []):
        if f.get("name") == "workOrders":
            work_orders_field = f
            break
    result["Query.workOrders"] = work_orders_field

    # 3. WorkOrderFilter input type (if it exists)
    filter_resp = _query(token, _INPUT_FIELDS_QUERY, {"name": "WorkOrderFilter"})
    result["WorkOrderFilter"] = (filter_resp.get("data") or {}).get("__type")

    print(json.dumps(result, indent=2, sort_keys=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())

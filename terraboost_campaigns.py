"""
terraboost_campaigns.py
=======================
READ-ONLY lookup against manage.terraboost.com's GraphQL API to populate
the SIO + art-file URL fields on the packing slip.

Endpoint: POST https://be-terraboost-v3.terraboost.com/graphql
Auth:     login(input: {email, password}) → token, then `Authorization: Bearer <token>`

Safety contract (enforced in code):
- ONE auth mutation is hardcoded (`_LOGIN_MUTATION`) — used only to get a token.
- All data calls go through `_query()`, which REFUSES any operation that doesn't
  start with the literal token "query " — making campaign edits, deletes,
  and art-file changes physically impossible from this module.

Public API:
    fetch_campaign_index(kids_tuple: tuple[str, ...]) -> dict[str, dict]

    Returns: {KID → {"sio": str, "top_file_url": str, "bottom_file_url": str,
                     "collection_name": str, "campaign_id": int}}
    For each KID, picks the campaignKiosk where today is within reservation dates.
    Cached for 1 hour via @st.cache_data so DCC doesn't hammer Terraboost.
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime
from typing import Iterable

import requests

try:
    import streamlit as st
    _HAS_STREAMLIT = True
except ImportError:
    _HAS_STREAMLIT = False

GQL_URL = "https://be-terraboost-v3.terraboost.com/graphql"
HTTP_TIMEOUT = 20


# -- Hardcoded auth mutation --------------------------------------------------
# This is the ONLY mutation this module ever sends. It does not write business
# data — it exchanges credentials for a session token.
_LOGIN_MUTATION = """mutation Login($email: String!, $password: String!) {
  login(input: {email: $email, password: $password}) {
    token
    refreshToken
  }
}"""


# -- Read-only data query -----------------------------------------------------
_KIDS_LOOKUP_QUERY = """query KidLookup($kids: [String!]) {
  kiosks(where: {importKioskId: {in: $kids}}) {
    importKioskId
    campaignKiosks {
      reservationStart
      reservationEnd
      printCollection {
        id
        collectionName
        topFileUrl
        bottomFileUrl
      }
      campaign {
        id
        orderNumber
        statusId
      }
    }
  }
}"""


# -- Token cache (process-wide, shared across Streamlit reruns) ---------------
if _HAS_STREAMLIT:
    @st.cache_resource(show_spinner=False)
    def _token_cache() -> dict:
        return {"token": None, "issued_ts": 0.0}
else:
    _LOCAL_CACHE = {"token": None, "issued_ts": 0.0}
    def _token_cache() -> dict:
        return _LOCAL_CACHE


def _login() -> str | None:
    """
    Calls the login mutation with credentials from env vars.
    Returns a bearer token string or None on failure.
    """
    email = os.environ.get("TERRABOOST_EMAIL")
    password = os.environ.get("TERRABOOST_PASSWORD")
    if not email or not password:
        return None
    try:
        r = requests.post(
            GQL_URL,
            headers={"content-type": "application/json"},
            json={"query": _LOGIN_MUTATION, "variables": {"email": email, "password": password}},
            timeout=HTTP_TIMEOUT,
        )
        if r.status_code != 200:
            return None
        body = r.json()
        if body.get("errors"):
            return None
        return ((body.get("data") or {}).get("login") or {}).get("token")
    except Exception:
        return None


def _get_token() -> str | None:
    """
    Returns a cached token, re-logging in every ~50 minutes (tokens are
    typically valid for 1hr, so we refresh slightly before expiry).
    """
    cache = _token_cache()
    if not cache.get("token") or (time.time() - cache.get("issued_ts", 0)) > 3000:
        new_token = _login()
        if new_token:
            cache["token"] = new_token
            cache["issued_ts"] = time.time()
    return cache.get("token")


def _query(query_str: str, variables: dict | None = None) -> dict:
    """
    READ-ONLY GraphQL call. Asserts the operation is a `query` block —
    refuses anything else. This is the safety net.
    """
    stripped = query_str.lstrip()
    if not stripped.startswith("query"):
        raise RuntimeError(
            "terraboost_campaigns is read-only — only `query` blocks allowed. "
            f"Got: {stripped[:40]!r}"
        )
    token = _get_token()
    if not token:
        return {}
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
        if r.status_code == 401:
            # Token expired — wipe cache, re-login once, retry
            cache = _token_cache()
            cache["token"] = None
            cache["issued_ts"] = 0.0
            token = _get_token()
            if not token:
                return {}
            headers["authorization"] = f"Bearer {token}"
            r = requests.post(GQL_URL, headers=headers,
                              json={"query": query_str, "variables": variables or {}},
                              timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return {}
        return r.json()
    except Exception:
        return {}


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _pick_active(campaign_kiosks: list, today: date) -> dict | None:
    """Return the campaignKiosk where today is within reservationStart..reservationEnd."""
    best = None
    best_start = None
    for ck in campaign_kiosks or []:
        start = _parse_date(ck.get("reservationStart"))
        if not start or start > today:
            continue
        end = _parse_date(ck.get("reservationEnd")) or date(2099, 1, 1)
        if today > end:
            continue
        # Pick the most recently-started one (in case of overlap)
        if best_start is None or start > best_start:
            best = ck
            best_start = start
    return best


def _do_fetch(kids: tuple) -> dict:
    """Inner fetch — pulled out so we can wrap it in @st.cache_data conditionally."""
    if not kids:
        return {}
    # Dedupe + drop blanks
    clean_kids = sorted({(k or "").strip() for k in kids if k and str(k).strip()})
    if not clean_kids:
        return {}
    resp = _query(_KIDS_LOOKUP_QUERY, {"kids": clean_kids})
    out: dict = {}
    today = date.today()
    for k in (resp.get("data") or {}).get("kiosks") or []:
        kid = k.get("importKioskId") or ""
        if not kid:
            continue
        active = _pick_active(k.get("campaignKiosks") or [], today)
        if not active:
            continue
        pc = active.get("printCollection") or {}
        cmp_ = active.get("campaign") or {}
        out[kid] = {
            "sio": str(cmp_.get("orderNumber") or ""),
            "campaign_id": cmp_.get("id"),
            "top_file_url": pc.get("topFileUrl") or "",
            "bottom_file_url": pc.get("bottomFileUrl") or "",
            "collection_name": pc.get("collectionName") or "",
        }
    return out


if _HAS_STREAMLIT:
    @st.cache_data(ttl=3600, show_spinner=False)
    def fetch_campaign_index(kids_tuple: tuple) -> dict:
        """
        For a tuple of OnFleet KIDs, returns the active-campaign metadata
        for each. Cached for 1 hour, shared across all dispatchers.
        """
        return _do_fetch(kids_tuple)
else:
    def fetch_campaign_index(kids_tuple: tuple) -> dict:
        return _do_fetch(kids_tuple)


# -- Smoke test (run this module directly: `python terraboost_campaigns.py`) -
if __name__ == "__main__":
    import json
    test_kids = ("91304B", "91304A", "95164A")
    print(f"Testing with KIDs: {test_kids}")
    if not os.environ.get("TERRABOOST_EMAIL") or not os.environ.get("TERRABOOST_PASSWORD"):
        print("Set TERRABOOST_EMAIL and TERRABOOST_PASSWORD env vars first.")
    else:
        result = fetch_campaign_index(test_kids)
        print(json.dumps(result, indent=2))

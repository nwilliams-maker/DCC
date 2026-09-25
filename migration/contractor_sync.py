from __future__ import annotations

import base64
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
import sqlalchemy as sa

BOARD_ID = int(os.environ.get("MONDAY_CONTRACTOR_BOARD_ID", "5840676529"))
MONDAY_API_URL = "https://api.monday.com/v2"

COLUMN_ALIASES = {
    "email": {"email", "email address", "e-mail", "e mail"},
    "name": {"name", "contractor", "contractor name", "ic", "ic name", "independent contractor"},
    "phone": {"phone", "phone number", "mobile", "cell", "cell phone"},
    "location": {"location", "address", "home location", "service area", "city state", "city/state"},
    "ic_list": {"ic list", "ic_list", "list", "contractor list"},
    "pod_color": {"pod color", "pod", "pod_color", "pod colour"},
    "digital_certified": {"digital certified", "digital certification", "digital_certified", "digital cert"},
    "unrestricted": {"unrestricted", "unrestricted ic", "full access"},
    "ic_status": {"ic status", "status", "contractor status"},
    "inactive_reason": {"reason for inactive status", "inactive reason", "reason inactive"},
}
TRUE_VALUES = {"yes", "y", "true", "1", "checked"}
FALSE_VALUES = {"no", "n", "false", "0", "unchecked"}
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


ONFLEET_API_URL = "https://onfleet.com/api/v2"

# Fallback pod assignment when Monday's Pod Color is blank/unmapped.
# This mirrors DCC's live pod geography so a newly added IC/FA can still
# become an OnFleet driver instead of sitting indefinitely in the retry queue.
STATE_TO_POD = {
    **{s: "Blue" for s in ("AL","AR","FL","IL","IA","LA","MI","MN","MS","MO","NC","SC","WI","OR","WA","NV")},
    **{s: "Green" for s in ("CO","DC","GA","IN","KY","MD","NJ","OH","UT")},
    **{s: "Orange" for s in ("AK","AZ","CA","HI","ID")},
    **{s: "Purple" for s in ("KS","MT","NE","NM","ND","OK","SD","TN","TX","WY")},
    **{s: "Red" for s in ("CT","DE","ME","MA","NH","NY","PA","RI","VT","VA","WV")},
}
STATE_NAME_TO_ABBR = {
    "ALABAMA":"AL","ALASKA":"AK","ARIZONA":"AZ","ARKANSAS":"AR","CALIFORNIA":"CA",
    "COLORADO":"CO","CONNECTICUT":"CT","DELAWARE":"DE","FLORIDA":"FL","GEORGIA":"GA",
    "HAWAII":"HI","IDAHO":"ID","ILLINOIS":"IL","INDIANA":"IN","IOWA":"IA","KANSAS":"KS",
    "KENTUCKY":"KY","LOUISIANA":"LA","MAINE":"ME","MARYLAND":"MD","MASSACHUSETTS":"MA",
    "MICHIGAN":"MI","MINNESOTA":"MN","MISSISSIPPI":"MS","MISSOURI":"MO","MONTANA":"MT",
    "NEBRASKA":"NE","NEVADA":"NV","NEW HAMPSHIRE":"NH","NEW JERSEY":"NJ","NEW MEXICO":"NM",
    "NEW YORK":"NY","NORTH CAROLINA":"NC","NORTH DAKOTA":"ND","OHIO":"OH","OKLAHOMA":"OK",
    "OREGON":"OR","PENNSYLVANIA":"PA","RHODE ISLAND":"RI","SOUTH CAROLINA":"SC",
    "SOUTH DAKOTA":"SD","TENNESSEE":"TN","TEXAS":"TX","UTAH":"UT","VERMONT":"VT",
    "VIRGINIA":"VA","WASHINGTON":"WA","WEST VIRGINIA":"WV","WISCONSIN":"WI","WYOMING":"WY",
    "DISTRICT OF COLUMBIA":"DC",
}

def _infer_pod_from_location(location: Any) -> str | None:
    text = str(location or "").strip().upper()
    if not text:
        return None
    # Prefer explicit USPS abbreviations.
    m = re.search(r"(?:,|\s)\s*([A-Z]{2})(?:\s+\d{5}(?:-\d{4})?|\s|,|$)", text)
    if m and m.group(1) in STATE_TO_POD:
        return STATE_TO_POD[m.group(1)]
    # Fall back to full state names.
    for name, abbr in STATE_NAME_TO_ABBR.items():
        if re.search(rf"\b{re.escape(name)}\b", text):
            return STATE_TO_POD.get(abbr)
    return None

def _resolved_pod(source: dict[str, Any]) -> str | None:
    return _clean_text(source.get("pod_color")) or _infer_pod_from_location(source.get("location"))


def _onfleet_headers() -> dict[str, str] | None:
    key = (os.environ.get("ONFLEET_KEY") or "").strip()
    if not key:
        return None
    token = base64.b64encode(f"{key}:".encode()).decode()
    return {"Authorization": f"Basic {token}", "Content-Type": "application/json"}


def _onfleet_request(method: str, path: str, **kwargs: Any) -> requests.Response:
    headers = _onfleet_headers()
    if not headers:
        raise RuntimeError("ONFLEET_KEY is not configured.")
    resp = requests.request(
        method,
        ONFLEET_API_URL + path,
        headers=headers,
        timeout=20,
        **kwargs,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"OnFleet {method} {path} failed ({resp.status_code}): {resp.text[:300]}")
    return resp


def _onfleet_list_workers() -> list[dict[str, Any]]:
    workers: list[dict[str, Any]] = []
    last_id = None
    seen: set[str] = set()
    for _ in range(50):
        path = "/workers" + (f"?lastId={last_id}" if last_id else "")
        payload = _onfleet_request("GET", path).json()
        page = payload if isinstance(payload, list) else (payload.get("workers") or [])
        if not page:
            break
        new_count = 0
        for worker in page:
            wid = str(worker.get("id") or "")
            if wid and wid not in seen:
                seen.add(wid)
                workers.append(worker)
                new_count += 1
        if new_count == 0:
            break
        last_id = page[-1].get("id")
        if not last_id:
            break
    return workers


def _onfleet_routing_destination_id(address: str) -> str:
    """Create an OnFleet Destination for a contractor's home/routing address."""
    address = _clean_text(address)
    if not address:
        raise RuntimeError("contractor address is missing")
    payload = {
        "address": {
            "unparsed": address,
        }
    }
    destination = _onfleet_request("POST", "/destinations", json=payload).json()
    destination_id = str(destination.get("id") or "").strip()
    if not destination_id:
        raise RuntimeError("OnFleet destination creation returned no id")
    return destination_id


def _worker_routing_address_present(worker: dict[str, Any]) -> bool:
    addresses = worker.get("addresses") or {}
    if not isinstance(addresses, dict):
        return False
    return bool(addresses.get("routing"))


def _onfleet_sync_new_contractor(source: dict[str, Any], teams: list[dict[str, Any]], workers: list[dict[str, Any]]) -> dict[str, Any]:
    """Reconcile a recent Monday contractor with Onfleet, without duplicate drivers."""
    pod = _resolved_pod(source)
    if not pod:
        return {"status": "failed", "reason": "pod could not be resolved from Pod Color or location/state"}

    pod_norm = _norm_title(pod)
    expected_team = f"pod: {pod_norm}"
    team = next(
        (t for t in teams if _norm_title(t.get("name")) == expected_team),
        None,
    )
    if not team:
        return {"status": "failed", "reason": f"OnFleet team POD: {pod} not found"}

    email = normalize_email(source.get("email"))
    phone = normalize_phone(source.get("phone"))
    worker = None
    for candidate in workers:
        c_phone = normalize_phone(candidate.get("phone"))
        c_email = normalize_email(candidate.get("email"))
        if (phone and c_phone == phone) or (email and c_email == email):
            worker = candidate
            break

    if worker is None:
        if not phone:
            return {"status": "failed", "reason": "valid phone required to create OnFleet driver"}
        address = _clean_text(source.get("location"))
        if not address:
            return {"status": "failed", "reason": "valid address required to create OnFleet driver"}
        routing_destination_id = _onfleet_routing_destination_id(address)
        payload = {
            "name": _clean_text(source.get("name")),
            "phone": "+1" + phone if len(phone) == 10 else phone,
            "teams": [team.get("id")],
            "addresses": {"routing": routing_destination_id},
            # Keep the original Monday text too so future syncs can detect
            # when the source address changed without geocoding comparisons.
            "metadata": [
                {"name": "Address", "type": "string", "value": address}
            ],
        }
        if email:
            payload["email"] = email
        worker = _onfleet_request("POST", "/workers", json=payload).json()
        workers.append({
            **worker,
            "phone": payload["phone"],
            "email": email,
            "teams": payload["teams"],
            "addresses": payload["addresses"],
            "metadata": payload["metadata"],
        })
        return {
            "status": "created",
            "worker_id": worker.get("id"),
            "team": team.get("name"),
            "routing_address_added": True,
        }

    worker_id = worker.get("id")
    if not worker_id:
        return {"status": "failed", "reason": "matched OnFleet driver has no id"}

    existing_team_ids = set(worker.get("teams") or [])
    target_team_id = team.get("id")
    update_payload: dict[str, Any] = {}
    if target_team_id and target_team_id not in existing_team_ids:
        existing_team_ids.add(target_team_id)
        update_payload["teams"] = list(existing_team_ids)

    address = _clean_text(source.get("location"))
    routing_address_added = False
    if address:
        existing_metadata = [
            m for m in (worker.get("metadata") or [])
            if _norm_title(m.get("name")) != "address"
        ]
        prior_source_address = next(
            (
                str(m.get("value") or "").strip()
                for m in (worker.get("metadata") or [])
                if _norm_title(m.get("name")) == "address"
            ),
            "",
        )

        # Backfill the real OnFleet routing/home address if absent. Also
        # refresh it when Monday's source address changes.
        if (not _worker_routing_address_present(worker)
                or _norm_title(prior_source_address) != _norm_title(address)):
            routing_destination_id = _onfleet_routing_destination_id(address)
            update_payload["addresses"] = {"routing": routing_destination_id}
            routing_address_added = True

        existing_metadata.append({"name": "Address", "type": "string", "value": address})
        update_payload["metadata"] = existing_metadata

    if update_payload:
        _onfleet_request("PUT", f"/workers/{worker_id}", json=update_payload)
        worker.update(update_payload)
        return {
            "status": "updated",
            "worker_id": worker_id,
            "team": team.get("name"),
            "address_added": bool(address),
            "routing_address_added": routing_address_added,
        }

    return {"status": "already_present", "worker_id": worker_id, "team": team.get("name")}


def _norm_title(value: Any) -> str:
    s = str(value or "").strip().lower().replace("_", " ")
    # Monday boards often prefix required columns with "*" (for example
    # "*email", "*phone", "*location"). Treat that as display decoration,
    # not part of the semantic column title.
    s = re.sub(r"^[^a-z0-9]+", "", s)
    return re.sub(r"\s+", " ", s)


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def normalize_email(value: Any) -> str | None:
    s = _clean_text(value)
    if not s:
        return None
    s = s.lower()
    return s if EMAIL_RE.match(s) else None


def normalize_phone(value: Any) -> str | None:
    s = _clean_text(value)
    if not s:
        return None
    digits = re.sub(r"\D", "", s)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits or None


def parse_bool(value: Any) -> bool | None:
    s = _norm_title(value)
    if s in TRUE_VALUES:
        return True
    if s in FALSE_VALUES:
        return False
    return None


def _preserve_blank(existing: Any, incoming: Any) -> Any:
    if isinstance(incoming, str):
        return incoming.strip() or existing
    return existing if incoming is None else incoming


def _monday_request(query: str, variables: dict[str, Any]) -> dict[str, Any]:
    token = (os.environ.get("MONDAY_API_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("MONDAY_API_TOKEN is not configured.")
    resp = requests.post(
        MONDAY_API_URL,
        headers={"Authorization": token, "Content-Type": "application/json"},
        json={"query": query, "variables": variables},
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("errors"):
        raise RuntimeError("Monday API error: " + "; ".join(str(e.get("message") or e) for e in payload["errors"]))
    return payload["data"]


def _discover_mapping(columns: list[dict[str, Any]]) -> dict[str, str]:
    by_title = {_norm_title(c.get("title")): str(c.get("id")) for c in columns}
    mapping: dict[str, str] = {}
    for field, aliases in COLUMN_ALIASES.items():
        matches = [by_title[a] for a in aliases if a in by_title]
        if matches:
            mapping[field] = matches[0]
    missing = [f for f in ("email",) if f not in mapping]
    if missing:
        raise RuntimeError(
            "Required Monday contractor columns could not be mapped by title: "
            + ", ".join(missing)
            + ". Available titles: "
            + ", ".join(sorted(by_title))
        )
    return mapping


def _fetch_board_rows() -> tuple[dict[str, str], list[dict[str, Any]]]:
    meta_q = """
    query($board:[ID!]!) {
      boards(ids:$board) {
        columns { id title type }
        items_page(limit:500) {
          cursor
          items { id name updated_at column_values { id text value } }
        }
      }
    }
    """
    data = _monday_request(meta_q, {"board": [BOARD_ID]})
    boards = data.get("boards") or []
    if not boards:
        raise RuntimeError(f"Monday board {BOARD_ID} was not found.")
    board = boards[0]
    mapping = _discover_mapping(board.get("columns") or [])
    page = board.get("items_page") or {}
    items = list(page.get("items") or [])
    cursor = page.get("cursor")
    next_q = """
    query($cursor:String!) {
      next_items_page(limit:500, cursor:$cursor) {
        cursor
        items { id name updated_at column_values { id text value } }
      }
    }
    """
    while cursor:
        nxt = _monday_request(next_q, {"cursor": cursor}).get("next_items_page") or {}
        items.extend(nxt.get("items") or [])
        cursor = nxt.get("cursor")
    return mapping, items


def _item_to_source(item: dict[str, Any], mapping: dict[str, str]) -> dict[str, Any]:
    vals = {str(v.get("id")): v for v in item.get("column_values") or []}
    def txt(field: str) -> str | None:
        col_id = mapping.get(field)
        if not col_id:
            return None
        v = vals.get(col_id) or {}
        return _clean_text(v.get("text"))
    source = {
        "monday_item_id": str(item.get("id") or ""),
        "monday_updated_at": item.get("updated_at"),
        "email": normalize_email(txt("email")),
        "name": txt("name") or _clean_text(item.get("name")),
        "phone": txt("phone"),
        "location": txt("location"),
        "ic_list": txt("ic_list"),
        "pod_color": txt("pod_color"),
        "digital_certified": parse_bool(txt("digital_certified")),
        "unrestricted": parse_bool(txt("unrestricted")),
        "ic_status": txt("ic_status"),
        "inactive_reason": txt("inactive_reason"),
    }
    availability = _availability_class(source)
    if availability:
        source["ic_list"] = availability
    return source


def _availability_class(source: dict[str, Any], insurance_window_days: int = 90) -> str | None:
    status = _norm_title(source.get("ic_status"))
    reason = _norm_title(source.get("inactive_reason"))

    if status == "active":
        return "ACTIVE"
    if status in {"new", "in training", "training"}:
        return "IN TRAINING"

    raw = source.get("monday_updated_at")
    recent = False
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        recent = dt >= datetime.now(timezone.utc) - timedelta(days=insurance_window_days)
    except Exception:
        recent = False

    # Monday has used several insurance-related labels over time. Treat any
    # recent insurance hold as route-eligible NEED INSURANCE whether the word
    # insurance appears in the status itself or in the inactive-reason field.
    insurance_related = ("insur" in status) or ("insur" in reason)
    inactive_related = (
        status == "inactive"
        or "inactive" in status
        or insurance_related
    )
    if inactive_related:
        if insurance_related and recent:
            return "NEED INSURANCE"
        return "INACTIVE"

    # Unknown/missing status is intentionally not considered route-eligible.
    return None


def _extract_state_zip_from_query(location: str) -> tuple[str | None, str | None]:
    text = str(location or "").upper()
    state = None
    zip_code = None

    # Prefer explicit USPS abbreviation near a ZIP / comma boundary.
    m_state = re.search(r"(?:,|\\s)\\s*([A-Z]{2})(?:\\s|,|$)", text)
    if m_state:
        state = m_state.group(1)

    m_zip = re.search(r"\\b(\\d{5})(?:-\\d{4})?\\b", text)
    if m_zip:
        zip_code = m_zip.group(1)

    return state, zip_code


def _mapbox_result_state_zip(feature: dict[str, Any]) -> tuple[str | None, str | None]:
    state = None
    zip_code = None

    # State/postcode may appear either as the feature itself or in context.
    parts = [feature] + list(feature.get("context") or [])
    for part in parts:
        pid = str(part.get("id") or "")
        if pid.startswith("region."):
            short = str((part.get("properties") or {}).get("short_code") or part.get("short_code") or "").upper()
            if short.startswith("US-") and len(short) >= 5:
                state = short[-2:]
            elif len(short) == 2:
                state = short
        elif pid.startswith("postcode."):
            txt = str(part.get("text") or "").strip()
            m = re.search(r"\\b(\\d{5})\\b", txt)
            if m:
                zip_code = m.group(1)

    return state, zip_code


def _mapbox_match_is_acceptable(location: str, feature: dict[str, Any]) -> bool:
    try:
        relevance = float(feature.get("relevance", 0) or 0)
    except Exception:
        relevance = 0.0

    # Reject weak/fuzzy first results. A complete street address should be
    # essentially exact; city/state-only locations can be slightly less exact.
    has_street_number = bool(re.search(r"\\b\\d{1,6}\\b", str(location or "")))
    min_relevance = 0.90 if has_street_number else 0.80
    if relevance < min_relevance:
        return False

    place_types = {str(x).lower() for x in (feature.get("place_type") or [])}
    if has_street_number and not (
        "address" in place_types
        or _clean_text(feature.get("address"))
    ):
        return False

    query_state, query_zip = _extract_state_zip_from_query(location)
    result_state, result_zip = _mapbox_result_state_zip(feature)

    # If Monday gives us a state/ZIP, do not accept a result that contradicts it.
    if query_state and result_state and query_state != result_state:
        return False
    if query_zip and result_zip and query_zip != result_zip:
        return False

    center = feature.get("center") or []
    if len(center) != 2:
        return False
    try:
        lng, lat = float(center[0]), float(center[1])
    except Exception:
        return False

    # U.S. sanity bounds, including Alaska/Hawaii. This mainly guards corrupt
    # API payloads / coordinate-order mistakes rather than doing state policing.
    if not (-179.9 <= lng <= -66.0 and 18.0 <= lat <= 72.0):
        return False

    return True


def _geocode(location: str | None) -> tuple[float | None, float | None]:
    token = (os.environ.get("MAPBOX_TOKEN") or "").strip()
    if not token or not location:
        return None, None
    try:
        resp = requests.get(
            "https://api.mapbox.com/geocoding/v5/mapbox.places/"
            + requests.utils.quote(location, safe="")
            + ".json",
            params={
                "access_token": token,
                "limit": 3,
                "country": "US",
                "autocomplete": "false",
            },
            timeout=15,
        )
        resp.raise_for_status()
        features = (resp.json() or {}).get("features") or []
        for feature in features:
            if not _mapbox_match_is_acceptable(location, feature):
                continue
            center = feature.get("center") or []
            return float(center[1]), float(center[0])
        # Fail closed: do not save coordinates for an ambiguous/weak match.
        return None, None
    except Exception:
        return None, None


def _build_update(existing: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for field in ("name", "phone", "location", "ic_list", "pod_color"):
        incoming = _clean_text(source.get(field))
        if incoming and incoming != existing.get(field):
            out[field] = incoming
    for field in ("digital_certified", "unrestricted"):
        incoming = source.get(field)
        if incoming is not None and incoming != existing.get(field):
            out[field] = bool(incoming)
    return out


def _recent_source(source: dict[str, Any], lookback_hours: int) -> bool:
    raw = source.get("monday_updated_at")
    if not raw:
        return True
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        return dt >= cutoff
    except Exception:
        # If Monday changes timestamp formatting, fail open rather than miss an IC.
        return True


def sync_contractors_from_monday(engine: sa.Engine | None = None) -> dict[str, Any]:
    if engine is None:
        db_url = (os.environ.get("DATABASE_URL") or "").strip()
        if not db_url:
            raise RuntimeError("DATABASE_URL is not configured.")
        engine = sa.create_engine(db_url, pool_pre_ping=True)

    mapping, items = _fetch_board_rows()
    all_sources = [_item_to_source(item, mapping) for item in items]
    lookback_hours = max(1, int(os.environ.get("MONDAY_SYNC_LOOKBACK_HOURS", "48")))
    sources = [src for src in all_sources if _recent_source(src, lookback_hours)]
    if not all_sources:
        raise RuntimeError("Monday contractor board returned no items; no database changes were made.")

    result = {
        "checked": len(sources),
        "skipped_old": max(0, len(all_sources) - len(sources)),
        "added": 0,
        "updated": 0,
        "unchanged": 0,
        "needs_review": 0,
        "failed": 0,
        "onfleet_created": 0,
        "onfleet_updated": 0,
        "onfleet_failed": 0,
        "details": [],
        "mapped_columns": sorted(mapping),
    }

    with engine.begin() as conn:
        conn.execute(sa.text("""
            CREATE TABLE IF NOT EXISTS contractor_onfleet_sync (
                email TEXT PRIMARY KEY,
                synced_at TIMESTAMPTZ
            )
        """))
        existing_rows = [
            dict(r)
            for r in conn.execute(
                sa.text(
                    """
                    SELECT id,email,name,location,phone,ic_list,lat,lng,pod_color,created_at,
                           digital_certified,unrestricted
                    FROM contractors
                    """
                )
            ).mappings().all()
        ]
        by_email = {normalize_email(r.get("email")): r for r in existing_rows if normalize_email(r.get("email"))}
        by_name_phone: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in existing_rows:
            key = (_norm_title(row.get("name")), normalize_phone(row.get("phone")) or "")
            if key[0] and key[1]:
                by_name_phone.setdefault(key, []).append(row)

        for source in sources:
            email = source.get("email")
            name = _clean_text(source.get("name"))
            if not email or not name:
                result["failed"] += 1
                result["details"].append({
                    "item_id": source.get("monday_item_id"),
                    "status": "failed",
                    "reason": "missing or invalid email/name",
                })
                continue

            existing = by_email.get(email)
            if existing is None:
                np_key = (_norm_title(name), normalize_phone(source.get("phone")) or "")
                candidates = by_name_phone.get(np_key, []) if np_key[1] else []
                if candidates and all(normalize_email(c.get("email")) != email for c in candidates):
                    result["needs_review"] += 1
                    result["details"].append({
                        "item_id": source.get("monday_item_id"),
                        "email": email,
                        "name": name,
                        "status": "needs_review",
                        "reason": "name+phone matches an existing contractor with a different email",
                    })
                    continue

                lat, lng = _geocode(source.get("location"))
                row = {
                    "email": email,
                    "name": name,
                    "location": _clean_text(source.get("location")),
                    "phone": _clean_text(source.get("phone")),
                    "ic_list": _clean_text(source.get("ic_list")),
                    "lat": lat,
                    "lng": lng,
                    "pod_color": _clean_text(source.get("pod_color")),
                    "digital_certified": bool(source.get("digital_certified")) if source.get("digital_certified") is not None else False,
                    "unrestricted": bool(source.get("unrestricted")) if source.get("unrestricted") is not None else False,
                }
                insert_res = conn.execute(
                    sa.text(
                        """
                        INSERT INTO contractors
                          (email,name,location,phone,ic_list,lat,lng,pod_color,digital_certified,unrestricted)
                        VALUES
                          (:email,:name,:location,:phone,:ic_list,:lat,:lng,:pod_color,:digital_certified,:unrestricted)
                        ON CONFLICT (email) DO NOTHING
                        """
                    ),
                    row,
                )
                if insert_res.rowcount:
                    result["added"] += 1
                    detail = {"email": email, "name": name, "status": "added"}
                    conn.execute(sa.text("""
                        INSERT INTO contractor_onfleet_sync (email) VALUES (:email)
                        ON CONFLICT (email) DO NOTHING
                    """), {"email": email})
                    result["details"].append(detail)
                else:
                    result["unchanged"] += 1
                continue

            updates = _build_update(existing, source)
            if "location" in updates or (
                _clean_text(source.get("location"))
                and (existing.get("lat") is None or existing.get("lng") is None)
            ):
                lat, lng = _geocode(updates.get("location") or source.get("location"))
                if lat is not None and lng is not None:
                    updates["lat"] = lat
                    updates["lng"] = lng
            if not updates:
                result["unchanged"] += 1
                continue

            sets = ", ".join(f"{k} = :{k}" for k in updates)
            params = dict(updates)
            params["id"] = existing["id"]
            conn.execute(
                sa.text(f"UPDATE contractors SET {sets}, updated_at = now() WHERE id = :id"),
                params,
            )
            result["updated"] += 1
            result["details"].append({
                "email": email,
                "name": name,
                "status": "updated",
                "fields": sorted(updates),
            })

    # The queue contains only inserts made by this sync, never historical
    # database imports. Failed Onfleet calls stay queued for the next run.
    with engine.connect() as conn:
        pending_emails = {
            row[0] for row in conn.execute(sa.text(
                "SELECT email FROM contractor_onfleet_sync WHERE synced_at IS NULL"
            ))
        }
    eligible = [src for src in all_sources
                if src.get("email") in pending_emails and src.get("name") and _resolved_pod(src)]
    # Reconcile the ENTIRE current route-eligible IC/FA roster against OnFleet.
    # Earlier versions only created workers for brand-new Postgres inserts and
    # merely *reported* recent DB rows missing from OnFleet. That left imported
    # or previously-synced contractors permanently stale. The Monday IC/FA board
    # is authoritative here: ACTIVE / IN TRAINING / NEED INSURANCE contractors
    # with a valid identity and resolvable pod should exist as OnFleet drivers.
    route_eligible = [
        src for src in all_sources
        if str(src.get("ic_list") or "").strip().upper()
           in {"ACTIVE", "IN TRAINING", "NEED INSURANCE"}
        and src.get("email")
        and src.get("name")
        and normalize_phone(src.get("phone"))
        and _resolved_pod(src)
    ]

    # Retain the prior 14-day audit metrics for visibility, but do not stop at
    # auditing: missing current drivers are now actually reconciled below.
    recent_db = {
        normalize_email(row.get("email")) for row in existing_rows
        if row.get("created_at") and row["created_at"] >= datetime.now(timezone.utc) - timedelta(days=14)
    }
    audit = [src for src in all_sources if src.get("email") in recent_db
             and src.get("name") and _resolved_pod(src)]
    result["onfleet_recent_db_audited"] = len(audit)
    result["onfleet_reconcile_candidates"] = len(route_eligible)

    if route_eligible:
        try:
            teams_payload = _onfleet_request("GET", "/teams").json()
            teams = teams_payload if isinstance(teams_payload, list) else (teams_payload.get("teams") or [])
            workers = _onfleet_list_workers()
        except Exception as exc:
            result["onfleet_failed"] += len(route_eligible)
            result["onfleet_error"] = str(exc)
            return result

        worker_phones = {normalize_phone(w.get("phone")) for w in workers}
        worker_emails = {normalize_email(w.get("email")) for w in workers}
        missing = [src for src in audit if
                   (not normalize_phone(src.get("phone")) or normalize_phone(src.get("phone")) not in worker_phones)
                   and src.get("email") not in worker_emails]
        result["onfleet_recent_db_missing"] = len(missing)
        result["onfleet_recent_db_missing_names"] = [src["name"] for src in missing[:20]]

        # Reconcile every current eligible contractor. _onfleet_sync_new_contractor
        # is idempotent: it creates when absent, updates team/address when needed,
        # and returns already_present without writing when OnFleet is current.
        for source in route_eligible:
            try:
                of_result = _onfleet_sync_new_contractor(source, teams, workers)
            except Exception as exc:
                of_result = {"status": "failed", "reason": str(exc)}
            status = of_result.get("status")
            if status in {"created", "updated", "failed"}:
                result[f"onfleet_{status}"] += 1
            if status in {"created", "updated", "already_present"}:
                with engine.begin() as conn:
                    conn.execute(sa.text("""
                        INSERT INTO contractor_onfleet_sync (email, synced_at)
                        VALUES (:email, now())
                        ON CONFLICT (email) DO UPDATE SET synced_at = EXCLUDED.synced_at
                    """), {"email": source["email"]})
            if status == "failed":
                result["details"].append({
                    "name": source["name"],
                    "status": "onfleet_failed",
                    "reason": of_result.get("reason"),
                })
    return result


def main() -> None:
    try:
        result = sync_contractors_from_monday()
        # Railway has a per-replica log rate limit; report a compact run summary.
        result["details"] = [d for d in result["details"] if d.get("status") in {"failed", "needs_review", "onfleet_failed"}]
        print(json.dumps(result, default=str))
    except Exception as exc:
        print(json.dumps({"error": str(exc)}, indent=2), file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()

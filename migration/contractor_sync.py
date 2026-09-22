from __future__ import annotations

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
    return source


def _availability_class(source: dict[str, Any], insurance_window_days: int = 90) -> str | None:
    status = _norm_title(source.get("ic_status"))
    reason = _norm_title(source.get("inactive_reason"))
    if status == "active":
        return "ACTIVE"
    if status == "new":
        return "IN TRAINING"
    if status == "inactive":
        raw = source.get("monday_updated_at")
        recent = False
        try:
            dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            recent = dt >= datetime.now(timezone.utc) - timedelta(days=insurance_window_days)
        except Exception:
            recent = False
        if "insur" in reason and recent:
            return "NEED INSURANCE"
        return "INACTIVE"
    # Unknown/missing status is intentionally not considered route-eligible.
    return None


def _geocode(location: str | None) -> tuple[float | None, float | None]:
    token = (os.environ.get("MAPBOX_TOKEN") or "").strip()
    if not token or not location:
        return None, None
    try:
        resp = requests.get(
            "https://api.mapbox.com/geocoding/v5/mapbox.places/"
            + requests.utils.quote(location, safe="")
            + ".json",
            params={"access_token": token, "limit": 1, "country": "US"},
            timeout=15,
        )
        resp.raise_for_status()
        features = (resp.json() or {}).get("features") or []
        if not features:
            return None, None
        center = features[0].get("center") or []
        if len(center) != 2:
            return None, None
        return float(center[1]), float(center[0])
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
        "details": [],
        "mapped_columns": sorted(mapping),
    }

    with engine.begin() as conn:
        existing_rows = [
            dict(r)
            for r in conn.execute(
                sa.text(
                    """
                    SELECT id,email,name,location,phone,ic_list,lat,lng,pod_color,
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
                conn.execute(
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
                result["added"] += 1
                result["details"].append({"email": email, "name": name, "status": "added"})
                continue

            updates = _build_update(existing, source)
            if "location" in updates:
                lat, lng = _geocode(updates["location"])
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

    return result


def main() -> None:
    try:
        result = sync_contractors_from_monday()
        print(json.dumps(result, indent=2, default=str))
    except Exception as exc:
        print(json.dumps({"error": str(exc)}, indent=2), file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()

"""
fn_utils.py — Terraboost Media Field Nation Utilities
All Field Nation logic lives here: manager mapping, upload generation, background saves.
"""

import io
import os
import sys
import threading
import requests
from datetime import datetime, timedelta
import csv

# Backend shared secret for GAS auth (security audit C3).
_GAS_AUTH = (os.environ.get("DCC_SHARED_SECRET") or "").strip()


# ---------------------------------------------------------------------------
# State → Work Order Manager (by pod)
# ---------------------------------------------------------------------------
FN_STATE_MANAGER = {
    # Orange Pod
    "AK": "Bernice Makaya", "AZ": "Bernice Makaya", "CA": "Bernice Makaya",
    "HI": "Bernice Makaya", "ID": "Bernice Makaya", "NV": "Bernice Makaya",
    "OR": "Bernice Makaya", "WA": "Bernice Makaya",
    # Green Pod
    "CO": "Reabetswe Segopa", "DC": "Reabetswe Segopa", "GA": "Reabetswe Segopa",
    "IN": "Reabetswe Segopa", "KY": "Reabetswe Segopa", "MD": "Reabetswe Segopa",
    "NJ": "Reabetswe Segopa", "OH": "Reabetswe Segopa", "UT": "Reabetswe Segopa",
    # Red Pod
    "CT": "Lee Adams", "DE": "Lee Adams", "MA": "Lee Adams", "ME": "Lee Adams",
    "NH": "Lee Adams", "NY": "Lee Adams", "PA": "Lee Adams", "RI": "Lee Adams",
    "VA": "Lee Adams", "VT": "Lee Adams", "WV": "Lee Adams",
    # Blue Pod
    "AL": "Elna Burger", "AR": "Elna Burger", "FL": "Elna Burger", "IA": "Elna Burger",
    "IL": "Elna Burger", "LA": "Elna Burger", "MI": "Elna Burger", "MN": "Elna Burger",
    "MO": "Elna Burger", "MS": "Elna Burger", "NC": "Elna Burger", "SC": "Elna Burger",
    "WI": "Elna Burger",
    # Purple Pod
    "KS": "Stacey Ferreira", "MT": "Stacey Ferreira", "ND": "Stacey Ferreira",
    "NE": "Stacey Ferreira", "NM": "Stacey Ferreira", "OK": "Stacey Ferreira",
    "SD": "Stacey Ferreira", "TN": "Stacey Ferreira", "TX": "Stacey Ferreira",
    "WY": "Stacey Ferreira",
}

PAY_PER_STOP = 20.0


# ---------------------------------------------------------------------------
# H13 — CSV formula-injection guard
# ---------------------------------------------------------------------------
# Excel/Sheets treat a cell whose first character is one of = + - @ (or a
# leading tab / carriage return) as a live formula when the CSV is opened.
# Venue / campaign / address values originate in external upstream systems,
# so any string cell written to an FN upload CSV is run through this guard.
_CSV_INJECTION_PREFIXES = ("=", "+", "-", "@", "\t", "\r")


def _csv_safe(value):
    """Neutralize CSV formula injection. If a string cell begins with a
    dangerous character, prefix it with a single quote so spreadsheet
    clients treat it as literal text. Non-strings pass through unchanged."""
    if not isinstance(value, str):
        return value
    if value and value[0] in _CSV_INJECTION_PREFIXES:
        return "'" + value
    return value


# ---------------------------------------------------------------------------
# M25 — structured address parsing (replaces fragile positional comma-split)
# ---------------------------------------------------------------------------
import re as _re_addr


def _parse_address(addr, task, fallback_city="", fallback_state=""):
    """Resolve (street, city, state, zip) for one FN CSV row.

    Prefers structured per-task fields (street/city/state/zip) when present.
    Falls back to parsing the comma-joined `full` address, but anchored from
    the END (state + zip are the last components) so a leading "Suite 4"
    segment no longer shifts city/state into the wrong column."""
    addr = addr or ""
    parts = [p.strip() for p in addr.split(",") if p.strip()]

    # Structured fields win when supplied by the task.
    street = str(task.get("street", "") or "").strip()
    city   = str(task.get("city", "") or "").strip()
    state  = str(task.get("state", "") or "").strip()
    zip_code = str(task.get("zip", "") or "").strip()

    # End-anchored parse: the last comma segment is typically "ST 12345" or
    # "ST" or "12345"; the second-to-last is the city.
    if parts:
        tail = parts[-1]
        m = _re_addr.match(r"^([A-Za-z]{2})\s+(\d{5}(?:-\d{4})?)$", tail)
        if m:
            if not state:
                state = m.group(1)
            if not zip_code:
                zip_code = m.group(2)
            if not city and len(parts) >= 2:
                city = parts[-2]
        elif _re_addr.match(r"^\d{5}(?:-\d{4})?$", tail):
            if not zip_code:
                zip_code = tail
            if not state and len(parts) >= 2:
                state = parts[-2]
            if not city and len(parts) >= 3:
                city = parts[-3]
        elif _re_addr.match(r"^[A-Za-z]{2}$", tail):
            if not state:
                state = tail
            if not city and len(parts) >= 2:
                city = parts[-2]
        else:
            # Legacy 4-part positional fallback.
            if not city and len(parts) > 1:
                city = parts[1]
            if not state and len(parts) > 2:
                state = parts[2]
            if not zip_code and len(parts) > 3:
                zip_code = parts[3]
        if not street:
            street = parts[0]

    if not street:
        street = addr
    if not city:
        city = fallback_city
    if not state:
        state = fallback_state
    return street, city, (state or "").strip().upper(), zip_code


# ---------------------------------------------------------------------------
# H16 — address dedup key normalization
# ---------------------------------------------------------------------------
def _norm_addr_key(addr):
    """Normalize an address string into a stable dedup key so the same venue
    written two slightly different ways ("123 Main St" vs "123 Main St.")
    collapses to one CSV row / one FN work order."""
    a = (addr or "").strip().lower()
    a = a.replace(".", " ")
    a = _re_addr.sub(r"\s+", " ", a)
    # Drop whitespace around commas so "st , dallas" and "st, dallas" match.
    a = _re_addr.sub(r"\s*,\s*", ", ", a)
    return a.strip().strip(",").strip()


# ---------------------------------------------------------------------------
# Background sheet save — never blocks the UI
# ---------------------------------------------------------------------------
def save_fn_to_sheet(gas_url: str, payload: dict, session_state=None) -> None:
    """Fire-and-forget: saves a route to the Field Nation Google Sheet tab.
    Clears the reverted flag from session_state after the write completes."""
    cluster_hash = payload.get("cluster_hash")

    def _worker():
        try:
            # May 18 2026 — bumped 15 → 90s. saveToFieldNation now does an inline
            # Monday placeholder push (find by address + 2 mutations per matched
            # item) on top of the FN sheet append, so multi-stop routes can take
            # 30–60s server-side. The thread is fire-and-forget so the user isn't
            # blocked either way, but the longer timeout means we'll actually see
            # errors via stderr instead of silently dropping the connection mid-push.
            requests.post(gas_url, json={"action": "saveToFieldNation", "payload": payload, "auth_secret": _GAS_AUTH}, timeout=90)
        except Exception as e:
            print(f"[fn_utils.save_fn_to_sheet] {type(e).__name__}: {e}", file=sys.stderr, flush=True)
        finally:
            # Clear reverted flag once sheet write is done (success or fail)
            if session_state is not None and cluster_hash:
                session_state.pop(f"reverted_{cluster_hash}", None)

    threading.Thread(target=_worker, daemon=True).start()


# ---------------------------------------------------------------------------
# FN CSV — internal row builder shared by single + combined generators
# ---------------------------------------------------------------------------
def _fmt_fn_date(d):
    """Format date as M/D/YYYY without leading zeros, cross-platform.
    Was previously using %-m/%-d which is Linux-only and raises ValueError on Windows."""
    return f"{d.month}/{d.day}/{d.year}"


def _fn_window():
    """Field Nation requires Start Date != End Date.
    Per dispatcher policy: Start = today + 2 days, End = today + 14 days."""
    try:
        _now = datetime.now()
        start_dt = _now + timedelta(days=2)
        end_dt   = _now + timedelta(days=14)
        return _fmt_fn_date(start_dt), _fmt_fn_date(end_dt)
    except Exception as e:
        print(f"[fn_utils._fn_window] {type(e).__name__}: {e}", file=sys.stderr, flush=True)
        return "", ""


def _fn_stop_rows(cluster: dict, start_date: str, end_date: str, bundle_number: int = 1):
    """Yield one CSV row per unique stop address in this cluster. Used by both
    generate_fn_upload (single cluster) and generate_combined_fn_upload (many).

    bundle_number: integer that becomes the value of the "Bundle" column for every
    row from this cluster. The combined generator passes 1, 2, 3, ... for each
    cluster so the dispatcher can sort/group all rows belonging to one route in the
    final spreadsheet. Single-cluster generate_fn_upload always passes 1."""
    stop_task_map: dict = {}
    for t in cluster.get('data', []):
        addr = t.get('full', '')
        if not addr:
            continue
        if addr not in stop_task_map:
            stop_task_map[addr] = []
        loc = t.get('location_in_venue', '').strip()
        existing = [x.get('location_in_venue', '') for x in stop_task_map[addr]]
        if loc not in existing:
            stop_task_map[addr].append(t)
    # Bundle column rule: only fill the Bundle # when this cluster has 2+
    # venue locations. Single-venue routes leave Bundle blank.
    _bundle_for_rows = bundle_number if len(stop_task_map) > 1 else ""
    for addr, tasks in stop_task_map.items():
        if not tasks:
            continue
        # M25 — structured/end-anchored parse instead of fixed positional split.
        street, city, state, zip_code = _parse_address(
            addr, tasks[0],
            fallback_city=cluster.get('city', ''),
            fallback_state=cluster.get('state', ''),
        )

        venue_name = tasks[0].get('venue_name', 'Terraboost Media')
        manager    = FN_STATE_MANAGER.get(state, '')

        base_row = [
            _bundle_for_rows,
            _csv_safe(venue_name),
            _csv_safe(street),
            _csv_safe(city),
            _csv_safe(state),
            _csv_safe(zip_code),
            "US",
            "Complete work anytime over a date range",
            start_date,
            "8:00 AM",
            end_date,
            "5:00 PM",
            "Fixed",
            PAY_PER_STOP,
            1.0,
            PAY_PER_STOP,
            _csv_safe(manager),
            "",
        ]

        custom_cols = []
        for slot_idx, task in enumerate(tasks[:5], 1):
            task_type    = str(task.get('task_type', 'Kiosk Install')).strip()
            loc_in_venue = str(task.get('location_in_venue', '')).strip()
            client       = str(task.get('client_company', '') or '').strip() or 'Terraboost Media'
            venue_id     = str(task.get('venue_id', '')).strip()
            combined_loc = f"{task_type} — {loc_in_venue}" if loc_in_venue else task_type

            custom_cols.append(_csv_safe(client))
            if slot_idx == 1:
                custom_cols.append(_csv_safe(venue_id))
            custom_cols.append(_csv_safe(combined_loc))

        # Pad empty slots up to 5
        filled = len(tasks[:5])
        for slot_idx in range(filled + 1, 6):
            custom_cols.append("")
            if slot_idx == 1:
                custom_cols.append("")
            custom_cols.append("")

        yield base_row + custom_cols


def _fn_csv_headers():
    base_headers = [
        "Bundle",
        "Location Name", "Address #1", "City", "State", "Postal Code", "Country",
        "Schedule Type", "Scheduled Start Date", "Scheduled Start Time",
        "Scheduled End Date", "Scheduled End Time", "Pay Type", "Pay Rate",
        "Approximate Hours to Complete", "Est. WO-Value", "Work Order Manager", "",
    ]
    custom_headers = []
    for n in range(1, 6):
        custom_headers.append(f"{n}. Customer Name")
        if n == 1:
            custom_headers.append("1. Venue ID")
        custom_headers.append(f"{n}. Location in Venue")
    return base_headers + custom_headers


# ---------------------------------------------------------------------------
# Mass upload file generator — single cluster (backward-compatible)
# ---------------------------------------------------------------------------
def generate_fn_upload(stop_metrics: dict, cluster: dict, due, final_pay: float, cluster_hash: str):
    """
    Generates a Field Nation mass upload CSV file for a SINGLE cluster.

    Returns:
      (BytesIO buffer, int stop_count)  or  (None, 0) if no kiosk stops found.
    """
    start_date, end_date = _fn_window()
    if not start_date:
        # Fall back to the route's due date if window calc fails.
        start_date = str(due)
        end_date   = str(due)

    rows = list(_fn_stop_rows(cluster, start_date, end_date))
    if not rows:
        return None, 0

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(_fn_csv_headers())
    writer.writerows(rows)

    bytes_buf = io.BytesIO(buf.getvalue().encode('utf-8'))
    bytes_buf.seek(0)
    return bytes_buf, len(rows)


# ---------------------------------------------------------------------------
# Mass upload file generator — combined (many clusters in one CSV)
# ---------------------------------------------------------------------------
def generate_combined_fn_upload(clusters: list):
    """
    Generates ONE Field Nation mass upload CSV containing rows from every cluster
    in `clusters`, DEDUPED BY ADDRESS — so the same venue posted across multiple
    DCC routes lands on FN.com as a single WO with combined kiosk slots instead
    of N duplicate WOs for the same location.

    Apr 27 2026 — added so the dispatcher can batch-export multiple FN routes into
    a single upload instead of downloading and stitching together N separate CSVs.

    May 17 2026 — Nick: "the field nation published routes are duplicated and I
    need them all combined into 1 work order per location address". Rewritten to
    group tasks by `full` address ACROSS all clusters (not per-cluster like
    before). Same address from 4 different routes → 1 CSV row → 1 FN WO.
    May 2026 — the Bundle column is now populated: each selected route gets a
    sequential bundle # (1, 2, ...) and every address row carries the # of the
    route that first contributed it. Field Nation bundles WOs sharing a value.

    Args:
        clusters: list of cluster dicts (same shape as generate_fn_upload's `cluster`).

    Returns:
        (BytesIO buffer, int total_stop_count, list[str] cluster_hashes_included).
        cluster_hashes_included only contains the hashes of clusters that actually
        contributed at least one task — clusters with zero kiosk-eligible tasks
        are silently skipped but their hashes still appear so the caller can mark
        them as exported.
    """
    start_date, end_date = _fn_window()
    if not start_date:
        # No good window — pick today / today+14 anyway as a safety fallback.
        _t = datetime.now()
        start_date = _fmt_fn_date(_t + timedelta(days=2))
        end_date   = _fmt_fn_date(_t + timedelta(days=14))

    # Aggregate every task from every cluster, keyed by `full` address.
    # Dedupe within each address by `location_in_venue` so the same kiosk
    # slot doesn't get listed twice when two routes both included it.
    addr_tasks: dict = {}        # norm key → list[task dict]
    addr_original: dict = {}     # norm key → first-seen original-cased address
    addr_city_state: dict = {}   # norm key → (city, state) for fallback header build
    addr_bundle: dict = {}       # norm key → bundle # of the route that first contributed it
    included_hashes: list = []
    # 🌟 BUNDLE # PER ROUTE (May 2026): each selected route gets a sequential
    # bundle number (1, 2, 3, ...). Field Nation's "Bundle" column groups WOs
    # sharing a value into one bundle on FN.com, so this makes each DCC route's
    # stops bundle together after upload. Because the combined CSV dedupes rows
    # by address across routes, an address that appears in two routes keeps the
    # number of whichever route listed it FIRST (addr_bundle.setdefault — first
    # write wins). enumerate starts at 1 so the column never shows a 0.
    for _bundle_idx, cluster in enumerate(clusters or [], start=1):
        ch = cluster.get('_cluster_hash') or cluster.get('cluster_hash') or ''
        included_hashes.append(ch)
        c_city  = cluster.get('city', '')
        c_state = cluster.get('state', '')
        for t in cluster.get('data', []):
            addr = t.get('full', '')
            if not addr:
                continue
            # H16 — dedupe on a normalized key so "123 Main St" and
            # "123 Main St." don't become two rows / two FN work orders.
            # Keep the first-seen original-cased address for CSV output.
            key = _norm_addr_key(addr)
            bucket = addr_tasks.setdefault(key, [])
            addr_original.setdefault(key, addr)
            addr_city_state.setdefault(key, (c_city, c_state))
            addr_bundle.setdefault(key, _bundle_idx)
            loc_in_venue = (t.get('location_in_venue', '') or '').strip()
            existing_locs = [(x.get('location_in_venue', '') or '').strip() for x in bucket]
            if loc_in_venue not in existing_locs:
                bucket.append(t)

    if not addr_tasks:
        return None, 0, included_hashes

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(_fn_csv_headers())

    total_stops = 0
    for key, tasks in addr_tasks.items():
        if not tasks:
            continue
        addr = addr_original.get(key, key)
        c_city, c_state = addr_city_state.get(key, ('', ''))
        # M25 — structured/end-anchored parse instead of fixed positional split.
        street, city, state, zip_code = _parse_address(
            addr, tasks[0], fallback_city=c_city, fallback_state=c_state,
        )

        venue_name = next((t.get('venue_name', '') for t in tasks if t.get('venue_name')), 'Terraboost Media')
        manager    = FN_STATE_MANAGER.get(state, '')

        base_row = [
            addr_bundle.get(key, ""),  # Bundle # — the route (1, 2, 3, ...)
                 # that first contributed this address. See addr_bundle build
                 # above. Field Nation groups WOs sharing a Bundle value into
                 # one bundle on FN.com, so each DCC route's stops bundle.
            _csv_safe(venue_name),
            _csv_safe(street),
            _csv_safe(city),
            _csv_safe(state),
            _csv_safe(zip_code),
            "US",
            "Complete work anytime over a date range",
            start_date,
            "8:00 AM",
            end_date,
            "5:00 PM",
            "Fixed",
            PAY_PER_STOP,
            1.0,
            PAY_PER_STOP,
            _csv_safe(manager),
            "",
        ]

        custom_cols = []
        for slot_idx, task in enumerate(tasks[:5], 1):
            task_type    = str(task.get('task_type', 'Kiosk Install')).strip()
            loc_in_venue = str(task.get('location_in_venue', '')).strip()
            client       = str(task.get('client_company', '') or '').strip() or 'Terraboost Media'
            venue_id     = str(task.get('venue_id', '')).strip()
            combined_loc = f"{task_type} — {loc_in_venue}" if loc_in_venue else task_type

            custom_cols.append(_csv_safe(client))
            if slot_idx == 1:
                custom_cols.append(_csv_safe(venue_id))
            custom_cols.append(_csv_safe(combined_loc))

        # Pad empty slots up to 5.
        filled = len(tasks[:5])
        for slot_idx in range(filled + 1, 6):
            custom_cols.append("")
            if slot_idx == 1:
                custom_cols.append("")
            custom_cols.append("")

        writer.writerow(base_row + custom_cols)
        total_stops += 1

    if total_stops == 0:
        return None, 0, included_hashes

    bytes_buf = io.BytesIO(buf.getvalue().encode('utf-8'))
    bytes_buf.seek(0)
    return bytes_buf, total_stops, included_hashes


# ---------------------------------------------------------------------------
# Assigned Provider — Field Nation provider name attached to a posted route
# ---------------------------------------------------------------------------
# May 16 2026 — Field Nation work orders get accepted on the FN platform by a
# specific provider. Their name gets typed into DCC (per-route in the FN
# Posted group) or pushed in bulk by a Chrome extension scraping FN.com.
# Either way it lands in the FN sheet row's JSON payload as `fn_provider`,
# survives reload, and travels with the route into Accepted via markFNAssigned.
# Display: card title reads "🌐 FN: <name>" when set, "🌐 FN" otherwise.

def save_fn_provider(gas_url, cluster_hash, provider_name, session_state=None):
    """Fire-and-forget: writes a single route's Assigned Provider to the FN
    sheet row's JSON payload (action=setFnProvider on the GAS side).

    Background-threaded so the UI doesn't wait on the HTTP round-trip. Mirrors
    the save_fn_to_sheet pattern.

    Args:
        gas_url: GAS Web App URL.
        cluster_hash: the route's cluster_hash (already on the FN sheet row).
        provider_name: free-form string from the dispatcher's input. Empty
            string clears the provider.
        session_state: optional Streamlit session_state for clearing any
            sync-pending flag once the write returns.
    """
    cluster_hash = str(cluster_hash or "").strip()
    provider_name = str(provider_name or "").strip()
    if not cluster_hash:
        return

    def _worker():
        try:
            requests.post(
                gas_url,
                json={
                    "action": "setFnProvider",
                    "cluster_hash": cluster_hash,
                    "provider_name": provider_name,
                    "auth_secret": _GAS_AUTH,
                },
                timeout=15,
            )
        except Exception as e:
            print(f"[fn_utils.save_fn_provider] {type(e).__name__}: {e}", file=sys.stderr, flush=True)
        finally:
            if session_state is not None and cluster_hash:
                session_state.pop(f"_pending_fn_provider_{cluster_hash}", None)

    threading.Thread(target=_worker, daemon=True).start()


def bulk_save_fn_providers_by_address(gas_url, address_provider_map):
    """Fire-and-forget bulk version: takes a dict of {address: provider_name}
    and POSTs to GAS, which matches each address against the stop addresses
    on every Posted FN sheet row and updates the row's fn_provider.

    Designed for the browser-extension path (Piece B): the extension scrapes
    FN.com for {address: assigned_provider} and posts the whole map at once.

    Args:
        gas_url: GAS Web App URL.
        address_provider_map: dict[str, str] — case-insensitive address keys.
    """
    if not isinstance(address_provider_map, dict) or not address_provider_map:
        return

    # Strip + normalize whitespace; drop empties.
    clean_map = {}
    for addr, name in address_provider_map.items():
        a = str(addr or "").strip()
        n = str(name or "").strip()
        if a and n:
            clean_map[a] = n
    if not clean_map:
        return

    def _worker():
        try:
            requests.post(
                gas_url,
                json={
                    "action": "bulkSetFnProvidersByAddress",
                    "address_map": clean_map,
                    "auth_secret": _GAS_AUTH,
                },
                timeout=30,
            )
        except Exception as e:
            print(f"[fn_utils.bulk_save_fn_providers_by_address] {type(e).__name__}: {e}", file=sys.stderr, flush=True)

    threading.Thread(target=_worker, daemon=True).start()


def extract_fn_provider(payload_or_dict):
    """Pull the Assigned Provider name off an FN row's JSON payload or off a
    cluster/ghost dict that's already been hydrated from one. Returns '' if
    not set so callers can do a clean falsy check.

    Args:
        payload_or_dict: dict (parsed JSON payload or cluster/ghost record).

    Returns:
        str: provider name, or empty string.
    """
    if not isinstance(payload_or_dict, dict):
        return ""
    val = payload_or_dict.get("fn_provider", "")
    return str(val or "").strip()


def format_fn_card_title(provider_name):
    """Render the FN portion of a card title: 'FN: <name>' if a provider is
    set, plain 'FN' otherwise. Caller is responsible for the leading 🌐.

    Kept tiny + pure so it can be reused anywhere the UI shows an FN-flagged
    route (FN tab Posted group, Accepted tab, etc.).
    """
    name = str(provider_name or "").strip()
    return f"FN: {name}" if name else "FN"

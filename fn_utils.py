"""
fn_utils.py — Terraboost Media Field Nation Utilities
All Field Nation logic lives here: manager mapping, upload generation, background saves.
"""

import io
import sys
import threading
import requests
from datetime import datetime, timedelta
import csv


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
# Background sheet save — never blocks the UI
# ---------------------------------------------------------------------------
def save_fn_to_sheet(gas_url: str, payload: dict, session_state=None) -> None:
    """Fire-and-forget: saves a route to the Field Nation Google Sheet tab.
    Clears the reverted flag from session_state after the write completes."""
    cluster_hash = payload.get("cluster_hash")

    def _worker():
        try:
            requests.post(gas_url, json={"action": "saveToFieldNation", "payload": payload}, timeout=15)
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

    for addr, tasks in stop_task_map.items():
        if not tasks:
            continue
        parts    = [p.strip() for p in addr.split(",")]
        street   = parts[0] if len(parts) > 0 else addr
        city     = parts[1] if len(parts) > 1 else cluster.get('city', '')
        state    = parts[2].strip().upper() if len(parts) > 2 else cluster.get('state', '')
        zip_code = tasks[0].get('zip', parts[3].strip() if len(parts) > 3 else '')

        venue_name = tasks[0].get('venue_name', 'Terraboost Media')
        manager    = FN_STATE_MANAGER.get(state, '')

        base_row = [
            bundle_number,
            venue_name,
            street,
            city,
            state,
            zip_code,
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
            manager,
            "",
        ]

        custom_cols = []
        for slot_idx, task in enumerate(tasks[:5], 1):
            task_type    = str(task.get('task_type', 'Kiosk Install')).strip()
            loc_in_venue = str(task.get('location_in_venue', '')).strip()
            client       = str(task.get('client_company', '') or '').strip() or 'Terraboost Media'
            venue_id     = str(task.get('venue_id', '')).strip()
            combined_loc = f"{task_type} — {loc_in_venue}" if loc_in_venue else task_type

            custom_cols.append(client)
            if slot_idx == 1:
                custom_cols.append(venue_id)
            custom_cols.append(combined_loc)

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
    in `clusters`. Each cluster's stops contribute their own row(s); headers appear
    once at the top.

    Apr 27 2026 — added so the dispatcher can batch-export multiple FN routes into
    a single upload instead of downloading and stitching together N separate CSVs.

    Args:
        clusters: list of cluster dicts (same shape as generate_fn_upload\'s `cluster`).

    Returns:
        (BytesIO buffer, int total_stop_count, list[str] cluster_hashes_included).
        cluster_hashes_included only contains the hashes of clusters that actually
        contributed rows — clusters with no kiosk-eligible stops are silently skipped
        but their hashes still appear so the caller can mark them as exported.
    """
    start_date, end_date = _fn_window()
    if not start_date:
        # No good window — pick today / today+14 anyway as a safety fallback.
        _t = datetime.now()
        start_date = _fmt_fn_date(_t + timedelta(days=2))
        end_date   = _fmt_fn_date(_t + timedelta(days=14))

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(_fn_csv_headers())

    total_stops = 0
    included_hashes = []
    # Number each cluster 1, 2, 3, ... so every row carries its parent route\'s
    # bundle index. Sort by Bundle in the final spreadsheet to group them back.
    for _bundle_idx, cluster in enumerate(clusters or [], start=1):
        rows = list(_fn_stop_rows(cluster, start_date, end_date, bundle_number=_bundle_idx))
        ch = cluster.get('_cluster_hash') or cluster.get('cluster_hash') or ''
        included_hashes.append(ch)
        if rows:
            writer.writerows(rows)
            total_stops += len(rows)

    if total_stops == 0:
        return None, 0, included_hashes

    bytes_buf = io.BytesIO(buf.getvalue().encode('utf-8'))
    bytes_buf.seek(0)
    return bytes_buf, total_stops, included_hashes

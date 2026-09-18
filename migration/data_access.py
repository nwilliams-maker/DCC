"""
New data-access layer for tactical_workspace_master_rw.py.

This module is meant to replace, one call site at a time (see
migration/README.md, step 6):
  - every `pandas.read_csv(f"{IC_SHEET_URL}/export?format=csv&gid=...")` read
  - every `requests.post(GAS_WEB_APP_URL, ...)` write

It intentionally mirrors the shape of the app's existing session-state
objects (a contractors DataFrame, a sent_dict/history_db-like structure) so
the swap at each call site is a substitution, not a rewrite of the
surrounding UI code. Wrap the app's existing @st.cache_data decorators
around these functions exactly as they wrap the CSV fetchers today.

NOT included here, and worth a deliberate decision before cutover (see the
migration doc's "Risks and open questions"): the OnFleet route-plan rename,
worker updates, and Monday.com mutations that GAS currently performs
server-side inside markFNAssigned. Those need their own home -- either
ported into Python functions called from mark_fn_assigned() below, or kept
as a separate small service -- they are NOT a side effect of the database
write itself.
"""
from __future__ import annotations

import json
from datetime import date
from typing import Any

import pandas as pd
import sqlalchemy as sa


# ---------------------------------------------------------------------------
# Contractors (replaces load_ic_database / _warm_load_ic_df / the inline
# gid=0 loader -- all three collapse into this one function)
# ---------------------------------------------------------------------------

def get_contractors(engine: sa.Engine) -> pd.DataFrame:
    """Returns the same shape as today's `ic_df`: one row per contractor,
    lowercase column names, ready to drop into st.session_state['ic_df']."""
    with engine.connect() as conn:
        return pd.read_sql(sa.text("SELECT * FROM contractors"), conn)


# ---------------------------------------------------------------------------
# Routes (replaces the Saved/Accepted/Declined/Finalized/Archive tabs and
# _cached_fetch_sent_records_from_sheet)
# ---------------------------------------------------------------------------

def get_routes(engine: sa.Engine, statuses: list[str] | None = None) -> list[dict[str, Any]]:
    """Returns one dict per route, `payload` already parsed back to a dict --
    equivalent to what the app gets today after `json.loads(row['json payload'])`."""
    query = "SELECT * FROM routes"
    params: dict[str, Any] = {}
    if statuses:
        # `status` is a Postgres ENUM (route_status); casting it to text lets
        # a plain text[] parameter match it without also having to cast the
        # bind parameter to route_status[] on the client side.
        query += " WHERE status::text = ANY(:statuses)"
        params["statuses"] = statuses
    with engine.connect() as conn:
        rows = conn.execute(sa.text(query), params).mappings().all()
    out = []
    for row in rows:
        d = dict(row)
        if isinstance(d.get("payload"), str):
            d["payload"] = json.loads(d["payload"])
        out.append(d)
    return out


def save_route(engine: sa.Engine, wo: str, contractor_name: str, payload: dict[str, Any]) -> None:
    """Replaces the `saveRoute` GAS action. Upsert on `wo` gives the same
    dedupe behavior GAS's 10-minute cluster_hash cache gave -- a retry with
    the same WO is a no-op update, not a duplicate insert."""
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT INTO routes (wo, contractor_name, status, comp, due, locs, stop_data, cluster_hash, payload)
                VALUES (:wo, :contractor_name, 'sent', :comp, :due, :locs, :stop_data, :cluster_hash, :payload)
                ON CONFLICT (wo) DO NOTHING
                """
            ),
            {
                "wo": wo,
                "contractor_name": contractor_name,
                "comp": payload.get("comp"),
                "due": payload.get("due"),
                "locs": json.dumps(payload.get("locs")) if payload.get("locs") is not None else None,
                "stop_data": json.dumps(payload.get("stopData")) if payload.get("stopData") is not None else None,
                "cluster_hash": payload.get("cluster_hash"),
                "payload": json.dumps(payload),
            },
        )
        _log_event(conn, wo, "saveRoute", payload)


def _set_route_status(engine_or_conn, wo: str, status: str, action: str, event_payload: dict[str, Any] | None = None) -> None:
    def _do(conn):
        conn.execute(
            sa.text("UPDATE routes SET status = :status, updated_at = now() WHERE wo = :wo"),
            {"status": status, "wo": wo},
        )
        _log_event(conn, wo, action, event_payload or {})

    if isinstance(engine_or_conn, sa.Engine):
        with engine_or_conn.begin() as conn:
            _do(conn)
    else:
        _do(engine_or_conn)


def archive_route(engine: sa.Engine, wo: str, reason: dict[str, Any] | None = None) -> None:
    """Replaces the `archiveRoute` GAS action."""
    _set_route_status(engine, wo, "archived", "archiveRoute", reason)


def finalize_route(engine: sa.Engine, wo: str) -> None:
    """Replaces the `finalizeRoute` GAS action."""
    _set_route_status(engine, wo, "finalized", "finalizeRoute")


def process_decision(engine: sa.Engine, wo: str, decision: str, signature: str, notes: str, phone: str) -> None:
    """Replaces the `processDecision` action that docs/portal-dcc-rw.html
    posts today. This needs its own small API endpoint (the static portal
    page can't hold a database credential) -- see the migration doc, "Code
    changes required" -- but the write itself is this function."""
    status = "accepted" if decision == "accept" else "declined"
    event_payload = {"decision": decision, "signature": signature, "notes": notes, "phone": phone}
    _set_route_status(engine, wo, status, "processDecision", event_payload)


def _log_event(conn, wo: str, action: str, payload: dict[str, Any]) -> None:
    conn.execute(
        sa.text(
            """
            INSERT INTO route_events (route_id, action, payload)
            SELECT id, :action, :payload FROM routes WHERE wo = :wo
            """
        ),
        {"wo": wo, "action": action, "payload": json.dumps(payload)},
    )


def get_changes_since(engine: sa.Engine, since_version: int) -> list[dict[str, Any]]:
    """Replaces the `getSyncVersion` GAS action / fetch_sync_status()."""
    with engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                """
                SELECT route_events.id AS version, routes.wo AS route_wo, route_events.action, route_events.payload,
                       route_events.created_at
                FROM route_events
                LEFT JOIN routes ON routes.id = route_events.route_id
                WHERE route_events.id > :since
                ORDER BY route_events.id
                """
            ),
            {"since": since_version},
        ).mappings().all()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Field Nation (replaces the Field Nation tab and its GAS actions)
# ---------------------------------------------------------------------------

def save_to_field_nation(engine: sa.Engine, work_order: str, payload: dict[str, Any]) -> None:
    """Replaces `saveToFieldNation` (fn_utils.save_fn_to_sheet)."""
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT INTO field_nation_orders (work_order, status, payload)
                VALUES (:work_order, 'posted', :payload)
                ON CONFLICT (work_order) DO NOTHING
                """
            ),
            {"work_order": work_order, "payload": json.dumps(payload)},
        )


def remove_field_nation(engine: sa.Engine, work_order: str) -> None:
    """Replaces `removeFieldNation`."""
    with engine.begin() as conn:
        conn.execute(sa.text("DELETE FROM field_nation_orders WHERE work_order = :wo"), {"wo": work_order})


def mark_fn_posted(engine: sa.Engine, work_order: str) -> None:
    """Replaces `markFNPosted`."""
    with engine.begin() as conn:
        conn.execute(
            sa.text("UPDATE field_nation_orders SET status = 'posted', updated_at = now() WHERE work_order = :wo"),
            {"wo": work_order},
        )


def mark_fn_assigned(engine: sa.Engine, work_order: str, route_plan_id: str | None = None) -> None:
    """Replaces `markFNAssigned`. Only updates the database row -- the
    OnFleet/Monday.com side effects GAS performs today are NOT included, see
    this module's docstring."""
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                UPDATE field_nation_orders
                SET status = 'assigned', route_plan_id = COALESCE(:route_plan_id, route_plan_id), updated_at = now()
                WHERE work_order = :wo
                """
            ),
            {"wo": work_order, "route_plan_id": route_plan_id},
        )


def set_fn_provider(engine: sa.Engine, work_order: str, provider: str) -> None:
    """Replaces `setFnProvider` (fn_utils.save_fn_provider)."""
    with engine.begin() as conn:
        conn.execute(
            sa.text("UPDATE field_nation_orders SET provider = :provider, updated_at = now() WHERE work_order = :wo"),
            {"wo": work_order, "provider": provider},
        )


def bulk_set_fn_providers_by_address(engine: sa.Engine, address_to_provider: dict[str, str]) -> int:
    """Replaces `bulkSetFnProvidersByAddress` (fn_utils.bulk_save_fn_providers_by_address).

    Matches on the `address` field inside each posted order's payload --
    port the exact matching/normalization logic from the GAS implementation
    before relying on this in production; this is a first draft."""
    updated = 0
    with engine.begin() as conn:
        rows = conn.execute(
            sa.text("SELECT id, payload FROM field_nation_orders WHERE status = 'posted'")
        ).mappings().all()
        for row in rows:
            payload = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
            address = payload.get("address")
            if address in address_to_provider:
                conn.execute(
                    sa.text("UPDATE field_nation_orders SET provider = :provider, updated_at = now() WHERE id = :id"),
                    {"provider": address_to_provider[address], "id": row["id"]},
                )
                updated += 1
    return updated


def set_fn_route_plan_id(engine: sa.Engine, work_order: str, route_plan_id: str) -> None:
    """Replaces `setFnRoutePlanId`."""
    with engine.begin() as conn:
        conn.execute(
            sa.text("UPDATE field_nation_orders SET route_plan_id = :rpid, updated_at = now() WHERE work_order = :wo"),
            {"wo": work_order, "rpid": route_plan_id},
        )


# ---------------------------------------------------------------------------
# Bundle maps (replaces GAS Script Properties saveBundleMap / loadBundleMap)
# ---------------------------------------------------------------------------

def save_bundle_map(engine: sa.Engine, pod: str, dispatcher_id: str, task_id_sets: Any) -> None:
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT INTO bundle_maps (pod, dispatcher_id, task_id_sets)
                VALUES (:pod, :dispatcher_id, :task_id_sets)
                ON CONFLICT (pod, dispatcher_id) DO UPDATE SET
                    task_id_sets = EXCLUDED.task_id_sets, updated_at = now()
                """
            ),
            {"pod": pod, "dispatcher_id": dispatcher_id, "task_id_sets": json.dumps(task_id_sets)},
        )


def load_bundle_map(engine: sa.Engine, pod: str, dispatcher_id: str) -> Any:
    with engine.connect() as conn:
        row = conn.execute(
            sa.text("SELECT task_id_sets FROM bundle_maps WHERE pod = :pod AND dispatcher_id = :dispatcher_id"),
            {"pod": pod, "dispatcher_id": dispatcher_id},
        ).fetchone()
    if not row:
        return None
    return row[0] if isinstance(row[0], (dict, list)) else json.loads(row[0])


# ---------------------------------------------------------------------------
# WO counters (replaces scanning the Archive tab for archived_wo suffixes)
# ---------------------------------------------------------------------------

def next_wo_suffix(engine: sa.Engine, contractor_name: str, wo_date: date) -> int:
    with engine.begin() as conn:
        row = conn.execute(
            sa.text(
                """
                INSERT INTO wo_counters (contractor_name, wo_date, next_suffix)
                VALUES (:name, :d, 2)
                ON CONFLICT (contractor_name, wo_date) DO UPDATE SET next_suffix = wo_counters.next_suffix + 1
                RETURNING next_suffix - 1
                """
            ),
            {"name": contractor_name, "d": wo_date},
        ).fetchone()
    return row[0]

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

2026-09-19 update: the three GAS side effects flagged below (OnFleet
auto-assign, OnFleet routePlan rename/worker updates, Monday.com mutations)
are now ported into migration/fn_side_effects.py, using the actual GAS
source (previously unavailable in this repo -- see that module's docstring
for where it came from) as the reference instead of the docstring summary
this module used to rely on. process_decision(), mark_fn_assigned(), and
save_to_field_nation() below call into it. What that porting effort also
surfaced, and fixed here: markFNAssigned doesn't just flip a status in GAS --
it MOVES the Field Nation sheet row into "Accepted routes", i.e. the order
becomes a full accepted route (shows in the Accepted bucket, goes through
the finalization checklist, etc.), not just a `field_nation_orders` status
change. mark_fn_assigned() below now also inserts the corresponding `routes`
row so that behavior isn't lost -- the original version of this function
(pre-2026-09-19) did not do this and was missing that entirely.

Still NOT done, and still worth a real staging test before removing the GAS
call sites in tactical_workspace_master_rw.py per the migration README's
"hard cutover" policy: this module's tests (migration/tests/) mock the
Onfleet/Monday HTTP calls, so the *shape* of every request is verified but
none of it has been run against a real Onfleet/Monday sandbox. Recommend one
supervised manual accept + one manual FN-assign against staging, watched live
in both Onfleet and Monday, before cutting the GAS path.
"""
from __future__ import annotations

import json
from datetime import date
from typing import Any

import pandas as pd
import sqlalchemy as sa

# tactical_workspace_master_rw.py imports this as `from migration import
# data_access`, while migration/tests/test_migration.py imports it as a bare
# top-level module with migration/ added to sys.path -- support both.
try:
    from . import fn_side_effects as _fx
except ImportError:
    import fn_side_effects as _fx

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

def process_decision(
    engine: sa.Engine,
    wo: str,
    decision: str,
    signature: str,
    notes: str,
    phone: str,
    task_ids: str = "",
    stop_order: str = "",
    comp: float | None = None,
) -> dict[str, Any]:
    """Replaces the `processDecision` action that docs/portal-dcc-rw.html
    posts today (see that file's fetch call for the exact field names this
    mirrors: phone/taskIds/stopOrder/comp/wo alongside decision/signature/
    notes). This needs its own small API endpoint (the static portal page
    can't hold a database credential) -- see the migration doc, "Code
    changes required".

    Beyond the status write, this now also runs the OnFleet auto-assign +
    ordered route creation on accept (migration/fn_side_effects.py,
    apply_onfleet_decision) -- ported from the live GAS processDecision so
    accepting a route here has the same effect it does today. Returns the
    same shape GAS's processDecision returns to the portal
    (onfleetSuccess/onfleetMsg/routeSuccess/routeMsg/partial/
    route_incomplete) so a future portal endpoint can pass it straight
    through, exactly like docs/portal-dcc-rw.html expects today."""
    target_status = "accepted" if decision == "accept" else "declined"

    with engine.connect() as conn:
        current = conn.execute(
            sa.text("SELECT status, payload FROM routes WHERE wo = :wo"), {"wo": wo}
        ).mappings().first()
    if not current:
        return {"success": False, "error": "Route not found."}

    current_status = current["status"]
    # Idempotent no-op: already in the target state (refresh-and-resubmit,
    # double-click) -- matches GAS's isInAccepted/isInDeclined short-circuit.
    if current_status == target_status:
        return {
            "success": True, "onfleetSuccess": True,
            "onfleetMsg": "Already recorded — no changes made.",
            "routeSuccess": False, "routeMsg": "",
        }
    # Block accepted -> declined: tasks are already assigned in Onfleet;
    # flipping back needs an explicit unassign, which the app's revoke flow
    # does properly -- matches GAS's block on this exact transition.
    if current_status == "accepted" and target_status == "declined":
        return {"success": False, "error": "This route was already accepted and pushed to Onfleet. Contact dispatch to change."}

    route_payload = current["payload"] if isinstance(current["payload"], dict) else json.loads(current["payload"])
    agreed_comp = route_payload.get("comp")
    try:
        agreed_comp = float(agreed_comp)
    except (TypeError, ValueError):
        agreed_comp = float(comp or 0)

    onfleet_result: dict[str, Any] = {"onfleetSuccess": True, "onfleetMsg": "", "routeSuccess": False, "routeMsg": "", "partial": False}
    if decision == "accept" and (task_ids or "").strip():
        try:
            onfleet_result = _fx.apply_onfleet_decision(
                decision=decision,
                task_ids=task_ids,
                wo=wo,
                phone=phone,
                comp=agreed_comp,
                due=route_payload.get("due"),
                digital_task_ids=route_payload.get("digitalTaskIds", ""),
                stop_order=stop_order,
            )
        except Exception as exc:  # noqa: BLE001 -- never let an Onfleet failure block the status write
            onfleet_result = {"onfleetSuccess": False, "onfleetMsg": f"Onfleet exception: {exc}", "routeSuccess": False, "routeMsg": "", "partial": True}

    event_payload = {
        "decision": decision, "signature": signature, "notes": notes, "phone": phone,
        "onfleet": onfleet_result,
    }
    _set_route_status(engine, wo, target_status, "processDecision", event_payload)

    return {"success": True, **onfleet_result}

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

def save_to_field_nation(engine: sa.Engine, work_order: str, payload: dict[str, Any], *, mirror_only: bool = False) -> dict[str, Any]:
    """Replaces `saveToFieldNation` (fn_utils.save_fn_to_sheet). Also pushes
    the Monday.com placeholder (installer="Field Nation") at each stop
    address, exactly like the live GAS action -- see
    fn_side_effects.push_fn_placeholder_to_monday. The DB write always
    happens even if the Monday push fails or is skipped (no MONDAY_API_TOKEN
    set); matches GAS's "any failure here CANNOT break the FN sheet save
    above" comment.

    mirror_only=True (2026-09-21, dual-write path): skips the
    fn_side_effects call entirely -- used when GAS has already performed the
    real saveToFieldNation live (Monday push and all) and this call only
    needs to mirror the resulting row into Postgres. This is the
    "mirror-only write path" migration/README.md flagged as missing before
    the Field Nation family could safely dual-write: without it, calling
    this function alongside GAS would have re-run push_fn_placeholder_to_monday
    a second time for the same order. mirror_save_to_field_nation() below is
    the intended entry point for that case -- don't pass mirror_only=True
    directly unless you have a specific reason to call save_to_field_nation()
    itself this way."""
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

    if mirror_only:
        return {"success": True, "monday": {"skipped": "mirror-only write -- GAS already performed the real Monday push live"}}

    try:
        monday_result = _fx.push_fn_placeholder_to_monday(payload, work_order)
    except Exception as exc:  # noqa: BLE001
        monday_result = {"skipped": f"exception: {exc}"}
    return {"success": True, "monday": monday_result}

def mirror_save_to_field_nation(engine: sa.Engine, work_order: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Dual-write mirror for saveToFieldNation (2026-09-21): writes the
    field_nation_orders row only, never calls into fn_side_effects -- GAS
    already pushed the real Monday.com placeholder live (or would have, if
    Monday.com weren't now retired -- see fn_side_effects.sync_monday_for_stops).
    Safe to call unconditionally right after (or alongside) the existing
    fn_utils.save_fn_to_sheet() GAS POST -- same best-effort,
    exception-swallowed pattern already proven for save_route/archive_route/
    finalize_route. No-ops harmlessly (ON CONFLICT DO NOTHING) on a retried
    POST for the same work_order."""
    return save_to_field_nation(engine, work_order, payload, mirror_only=True)

def remove_field_nation(engine: sa.Engine, work_order: str) -> None:
    """Replaces `removeFieldNation`."""
    with engine.begin() as conn:
        conn.execute(sa.text("DELETE FROM field_nation_orders WHERE work_order = :wo"), {"wo": work_order})

def mirror_remove_field_nation_by_cluster_hash(engine: sa.Engine, cluster_hash: str) -> dict[str, Any]:
    """Dual-write mirror for removeFieldNation (2026-09-21), for
    background_fn_revoke()'s call site in tactical_workspace_master_rw.py,
    which only has cluster_hash in scope. removeFieldNation has no
    Onfleet/Monday side effect in GAS either -- remove_field_nation() never
    called fn_side_effects -- so there's no mirror_only distinction to make
    here, just the same cluster_hash -> work_order resolution the other FN
    mirrors use. No-ops harmlessly (returns a `skipped` result, never
    raises) if no matching row exists yet."""
    with engine.connect() as conn:
        work_order = _fn_work_order_for_cluster_hash(conn, cluster_hash)
    if not work_order:
        return {"success": False, "skipped": f"no field_nation_orders row found for cluster_hash={cluster_hash!r}"}
    remove_field_nation(engine, work_order)
    return {"success": True, "work_order": work_order}

def mark_fn_posted(engine: sa.Engine, work_order: str) -> None:
    """Replaces `markFNPosted`."""
    with engine.begin() as conn:
        conn.execute(
            sa.text("UPDATE field_nation_orders SET status = 'posted', updated_at = now() WHERE work_order = :wo"),
            {"wo": work_order},
        )

def mirror_mark_fn_posted_by_cluster_hash(engine: sa.Engine, cluster_hash: str) -> dict[str, Any]:
    """Dual-write mirror for markFNPosted (2026-09-21). Same cluster_hash
    resolution as the other FN mirrors. GAS's markFNPosted accepts a
    comma-separated list of cluster_hashes for the app's two bulk call
    sites (bulk-pod, bulk-digital) -- this function takes exactly one, so
    callers loop and call it once per hash rather than passing a CSV string
    here. No-ops harmlessly if no matching row exists yet."""
    with engine.connect() as conn:
        work_order = _fn_work_order_for_cluster_hash(conn, cluster_hash)
    if not work_order:
        return {"success": False, "skipped": f"no field_nation_orders row found for cluster_hash={cluster_hash!r}"}
    mark_fn_posted(engine, work_order)
    return {"success": True, "work_order": work_order}

def mark_fn_assigned(engine: sa.Engine, work_order: str, route_plan_id: str | None = None, *, mirror_only: bool = False) -> dict[str, Any]:
    """Replaces `markFNAssigned`.

    GAS's markFNAssigned does more than flip a status: it MOVES the Field
    Nation sheet row into "Accepted routes" -- renaming the WO to
    "FN-<Provider>-<MMDD>" along the way -- so the order becomes a full
    accepted route from that point on (Accepted bucket, finalization
    checklist, everything `get_routes()` callers expect). The original
    version of this function (before 2026-09-19, written without the actual
    GAS source available) only updated `field_nation_orders` in place and
    did not replicate that move -- a real functional gap, not just a missing
    side effect, since nothing would ever promote the order into `routes`.
    Fixed here: this now also inserts the corresponding `routes` row.
    `field_nation_orders` is kept (status='assigned') for FN-specific
    tracking rather than deleted, since Postgres doesn't need the
    delete-to-move dance a spreadsheet does.

    Also runs the OnFleet routePlan rename + per-task metadata/worker re-PUT
    and the Monday.com address-matched sync (fn_side_effects.py), exactly
    like the live GAS action -- see that module's docstring for the source.

    mirror_only=True (2026-09-21, dual-write path): skips
    fn_side_effects.apply_fn_assigned_side_effects() entirely -- no OnFleet
    routePlan rename/re-PUT, no Monday sync -- because GAS already performed
    those live. Everything else (the new_wo computation, the
    field_nation_orders update, the routes insert/upsert, the event log)
    still runs exactly as it would live, so the Postgres row ends up
    identical to what the non-mirror call would produce, just without
    re-triggering the side effects a second time. mirror_mark_fn_assigned()
    / mirror_mark_fn_assigned_by_cluster_hash() below are the intended entry
    points -- don't pass mirror_only=True directly unless you have a
    specific reason to call mark_fn_assigned() itself this way."""
    with engine.begin() as conn:
        row = conn.execute(
            sa.text("SELECT payload, provider, route_plan_id FROM field_nation_orders WHERE work_order = :wo"),
            {"wo": work_order},
        ).mappings().first()
        if not row:
            return {"success": False, "error": "Route hash not found in Field Nation tab."}

        payload = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
        provider = (row["provider"] or payload.get("fn_provider") or "").strip()
        legacy_wo = str(payload.get("wo") or work_order)

        from datetime import datetime as _dt
        date_stamp = _dt.now().strftime("%-m/%-d")
        if provider:
            new_wo = f"FN-{' '.join(provider.split())}-{date_stamp}"
        else:
            new_wo = legacy_wo or f"FN-Unassigned-{date_stamp}"

        payload = dict(payload)
        payload["assigned_to_fn"] = True
        payload["fn_assigned_ts"] = _dt.utcnow().isoformat() + "Z"
        payload["wo"] = new_wo

        side_effects: dict[str, Any] = {"partial": False, "partialReason": "", "routePlanId": route_plan_id or row["route_plan_id"]}
        if not mirror_only:
            try:
                side_effects = _fx.apply_fn_assigned_side_effects(payload, new_wo, provider, route_plan_id or row["route_plan_id"])
            except Exception as exc:  # noqa: BLE001 -- sheet/row move already committed logically below; never block on this
                side_effects = {"partial": True, "partialReason": f"side-effect exception: {exc}", "routePlanId": route_plan_id or row["route_plan_id"]}

        resolved_route_plan_id = side_effects.get("routePlanId") or route_plan_id or row["route_plan_id"]

        conn.execute(
            sa.text(
                """
                UPDATE field_nation_orders
                SET status = 'assigned', route_plan_id = :route_plan_id, payload = :payload, updated_at = now()
                WHERE work_order = :wo
                """
            ),
            {"wo": work_order, "route_plan_id": resolved_route_plan_id, "payload": json.dumps(payload)},
        )
        conn.execute(
            sa.text(
                """
                INSERT INTO routes (wo, contractor_name, status, comp, due, locs, stop_data, cluster_hash, payload)
                VALUES (:wo, 'Field Nation', 'accepted', :comp, :due, :locs, :stop_data, :cluster_hash, :payload)
                ON CONFLICT (wo) DO UPDATE SET
                    status = 'accepted', payload = EXCLUDED.payload, updated_at = now()
                """
            ),
            {
                "wo": new_wo,
                "comp": payload.get("comp"),
                "due": payload.get("due"),
                "locs": json.dumps(payload.get("locs")) if payload.get("locs") is not None else None,
                "stop_data": json.dumps(payload.get("stopData")) if payload.get("stopData") is not None else None,
                "cluster_hash": payload.get("cluster_hash"),
                "payload": json.dumps(payload),
            },
        )
        _log_event(conn, new_wo, "markFNAssigned", {"work_order": work_order, "provider": provider, "side_effects": side_effects})

    return {"success": True, "wo": new_wo, "partial": side_effects.get("partial", False), "partialReason": side_effects.get("partialReason", "")}

def mirror_mark_fn_assigned(engine: sa.Engine, work_order: str, route_plan_id: str | None = None) -> dict[str, Any]:
    """Dual-write mirror for markFNAssigned (2026-09-21), for a caller that
    already knows the field_nation_orders `work_order` (the WO
    mirror_save_to_field_nation wrote it under). Today's live call sites in
    tactical_workspace_master_rw.py only have `cluster_hash` in scope --
    use mirror_mark_fn_assigned_by_cluster_hash() for those."""
    return mark_fn_assigned(engine, work_order, route_plan_id, mirror_only=True)

def _fn_work_order_for_cluster_hash(conn, cluster_hash: str) -> str | None:
    """Looks up the field_nation_orders.work_order whose payload was stamped
    with this cluster_hash by mirror_save_to_field_nation() (fn_payload
    always includes "cluster_hash" -- see the app's FN posting call site).
    Postgres's ->> operator matches Sheet-based GAS's own by-cluster_hash
    lookup for the same row. Most recent match wins on the rare chance the
    same cluster_hash was posted more than once (shouldn't happen -- WOs are
    unique -- but this is a read-only lookup, not a write, so it's cheap
    insurance rather than something worth hard-failing on)."""
    row = conn.execute(
        sa.text(
            "SELECT work_order FROM field_nation_orders WHERE payload ->> 'cluster_hash' = :ch "
            "ORDER BY created_at DESC LIMIT 1"
        ),
        {"ch": cluster_hash},
    ).fetchone()
    return row[0] if row else None

def mirror_mark_fn_assigned_by_cluster_hash(engine: sa.Engine, cluster_hash: str, route_plan_id: str | None = None) -> dict[str, Any]:
    """Dual-write entry point for the live app's three markFNAssigned call
    sites (single-route, bulk-pod, bulk-digital in
    tactical_workspace_master_rw.py), which only have `cluster_hash` in
    scope -- the WO that mirror_save_to_field_nation() wrote the
    field_nation_orders row under isn't otherwise available to them. Looks
    that WO up (see _fn_work_order_for_cluster_hash), then delegates to
    mirror_mark_fn_assigned(). No-ops harmlessly -- returns a `skipped`
    result, never raises -- if no matching posted FN order is in Postgres
    yet (e.g. this route was posted to FN before the saveToFieldNation
    mirror existed, so there's nothing here for markFNAssigned to mirror
    onto)."""
    with engine.connect() as conn:
        work_order = _fn_work_order_for_cluster_hash(conn, cluster_hash)
    if not work_order:
        return {"success": False, "skipped": f"no field_nation_orders row found for cluster_hash={cluster_hash!r}"}
    return mirror_mark_fn_assigned(engine, work_order, route_plan_id)

def set_fn_provider(engine: sa.Engine, work_order: str, provider: str) -> None:
    """Replaces `setFnProvider` (fn_utils.save_fn_provider). GAS stamps
    `fn_provider`/`fn_provider_ts` INTO the JSON payload (not a separate
    column) -- mark_fn_assigned() later reads `payload.fn_provider` the same
    way GAS's markFNAssigned does, so this keeps both the dedicated
    `provider` column (handy for the app's own queries) and the payload
    field in sync rather than only the column, which was the gap here
    before: mark_fn_assigned would never have seen a provider that was only
    ever written to the column."""
    from datetime import datetime as _dt
    with engine.begin() as conn:
        row = conn.execute(
            sa.text("SELECT payload FROM field_nation_orders WHERE work_order = :wo"), {"wo": work_order}
        ).mappings().first()
        payload = {}
        if row:
            payload = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
        payload = dict(payload)
        payload["fn_provider"] = provider
        payload["fn_provider_ts"] = _dt.utcnow().isoformat() + "Z"
        conn.execute(
            sa.text(
                "UPDATE field_nation_orders SET provider = :provider, payload = :payload, updated_at = now() WHERE work_order = :wo"
            ),
            {"wo": work_order, "provider": provider, "payload": json.dumps(payload)},
        )

def mirror_set_fn_provider_by_cluster_hash(engine: sa.Engine, cluster_hash: str, provider: str) -> dict[str, Any]:
    """Dual-write mirror for setFnProvider (2026-09-21), for
    fn_utils.save_fn_provider()'s call site, which only has cluster_hash in
    scope. No-ops harmlessly if no matching row exists yet."""
    with engine.connect() as conn:
        work_order = _fn_work_order_for_cluster_hash(conn, cluster_hash)
    if not work_order:
        return {"success": False, "skipped": f"no field_nation_orders row found for cluster_hash={cluster_hash!r}"}
    set_fn_provider(engine, work_order, provider)
    return {"success": True, "work_order": work_order}

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

def mirror_set_fn_route_plan_id_by_cluster_hash(engine: sa.Engine, cluster_hash: str, route_plan_id: str) -> dict[str, Any]:
    """Dual-write mirror for setFnRoutePlanId (2026-09-21), for
    assign_tasks_to_fn_team()'s call site in tactical_workspace_master_rw.py,
    which only has cluster_hash in scope. No-ops harmlessly if no matching
    row exists yet."""
    with engine.connect() as conn:
        work_order = _fn_work_order_for_cluster_hash(conn, cluster_hash)
    if not work_order:
        return {"success": False, "skipped": f"no field_nation_orders row found for cluster_hash={cluster_hash!r}"}
    set_fn_route_plan_id(engine, work_order, route_plan_id)
    return {"success": True, "work_order": work_order}

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

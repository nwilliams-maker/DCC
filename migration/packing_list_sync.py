from __future__ import annotations

import io
import json
import os
import re
from datetime import datetime, timezone
from typing import Any

import requests
import pandas as pd
import sqlalchemy as sa
from reportlab.lib import colors
from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

TB_GQL = "https://be-terraboost-v3.terraboost.com/graphql"
MONDAY_GQL = "https://api.monday.com/v2"
MONDAY_FILE_GQL = "https://api.monday.com/v2/file"

PRINT_STATUS_BOARD_ID = int(os.environ.get("PRINT_STATUS_BOARD_ID", "6920657806"))
PRINT_PACKING_LIST_COLUMN_ID = os.environ.get("PRINT_PACKING_LIST_COLUMN_ID", "files__1")
POD_FILTER = (os.environ.get("POD_FILTER") or "Orange").strip()
NOT_BEFORE = (os.environ.get("PACKING_SYNC_NOT_BEFORE") or "").strip()
TEST_WO = (os.environ.get("PACKING_SYNC_TEST_WO") or "").strip()


def _clean(v: Any) -> str:
    return "" if v is None else str(v).strip()


def tb_login() -> str:
    email = _clean(os.environ.get("TERRABOOST_EMAIL"))
    password = _clean(os.environ.get("TERRABOOST_PASSWORD"))
    if not email or not password:
        raise RuntimeError("Terraboost credentials are not configured")
    q = """mutation Login($email: String!, $password: String!) {
      login(input: {email: $email, password: $password}) { token }
    }"""
    r = requests.post(TB_GQL, json={"query": q, "variables": {"email": email, "password": password}}, timeout=30)
    r.raise_for_status()
    body = r.json()
    if body.get("errors"):
        raise RuntimeError("Terraboost login failed")
    token = _clean((((body.get("data") or {}).get("login") or {}).get("token")))
    if not token:
        raise RuntimeError("Terraboost login returned no token")
    return token


def tb_query(token: str, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    r = requests.post(
        TB_GQL,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"query": query, "variables": variables or {}},
        timeout=45,
    )
    r.raise_for_status()
    body = r.json()
    if body.get("errors"):
        raise RuntimeError("; ".join(_clean(e.get("message") or e) for e in body["errors"]))
    return body.get("data") or {}


def monday_query(query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    token = _clean(os.environ.get("MONDAY_API_TOKEN"))
    if not token:
        raise RuntimeError("MONDAY_API_TOKEN is not configured")
    r = requests.post(
        MONDAY_GQL,
        headers={"Authorization": token, "Content-Type": "application/json"},
        json={"query": query, "variables": variables or {}},
        timeout=45,
    )
    r.raise_for_status()
    body = r.json()
    if body.get("errors"):
        raise RuntimeError("; ".join(_clean(e.get("message") or e) for e in body["errors"]))
    return body.get("data") or {}


def _orange_from_payload(payload: dict[str, Any]) -> bool:
    orange_states = {"AK", "AZ", "CA", "HI", "ID", "NV", "OR", "WA"}
    state = _clean(payload.get("state")).upper()[:2]
    if state in orange_states:
        return True
    locs = _clean(payload.get("locs"))
    if locs:
        for stop in [x.strip() for x in locs.split("|") if x.strip()]:
            m = re.search(r",\s*([A-Za-z]{2})\s+\d{5}(?:-\d{4})?\s*$", stop)
            if m and m.group(1).upper() in orange_states:
                return True
    return False


def get_orange_accepted_routes() -> list[dict[str, Any]]:
    """Read the same Accepted Routes source DCC uses.

    Accepted decisions are still authoritative in the Google Sheet tab
    (gid 934075207); Postgres does not currently mirror processDecision.
    """
    sheet_url = _clean(os.environ.get("IC_SHEET_URL"))
    if not sheet_url:
        raise RuntimeError("IC_SHEET_URL is not configured")
    base = sheet_url.split("/edit")[0]
    url = f"{base}/export?format=csv&gid=934075207"
    df = pd.read_csv(url)
    df.columns = [str(c).strip().lower() for c in df.columns]
    if "json payload" not in df.columns:
        raise RuntimeError("Accepted Routes sheet has no 'json payload' column")

    out: list[dict[str, Any]] = []
    cutoff = pd.to_datetime(NOT_BEFORE, utc=True, errors="coerce") if NOT_BEFORE else None
    for _, row in df.iterrows():
        raw_payload = row.get("json payload")
        if pd.isna(raw_payload) or not _clean(raw_payload):
            continue
        try:
            payload = json.loads(str(raw_payload))
        except Exception:
            continue
        wo = _clean(payload.get("wo"))
        if not wo:
            continue
        if TEST_WO and wo != TEST_WO:
            continue
        if not _orange_from_payload(payload):
            continue

        raw_date = row.get("date created")
        dt = pd.to_datetime(raw_date, utc=True, errors="coerce")
        if cutoff is not None and pd.notna(dt) and dt < cutoff:
            continue

        out.append({
            "id": None,
            "wo": wo,
            "contractor_name": _clean(row.get("contractor")) or _clean(payload.get("contractor")) or "Unknown Contractor",
            "contractor_id": None,
            "updated_at": dt.isoformat() if pd.notna(dt) else _clean(raw_date),
            "payload": payload,
            "stop_data": payload.get("stopData"),
            "pod_color": "Orange",
        })

    out.sort(key=lambda x: _clean(x.get("updated_at")))
    return out


def monday_board_meta() -> dict[str, Any]:
    q = """query($ids:[ID!]!) {
      boards(ids:$ids) {
        id
        name
        groups { id title position }
        columns { id title type }
      }
    }"""
    boards = monday_query(q, {"ids": [PRINT_STATUS_BOARD_ID]}).get("boards") or []
    if not boards:
        raise RuntimeError("Print Status board not found")
    return boards[0]


def choose_target_group(board: dict[str, Any]) -> str | None:
    forced = _clean(os.environ.get("PRINT_STATUS_GROUP_ID"))
    if forced:
        return forced
    groups = board.get("groups") or []
    for g in groups:
        title = _clean(g.get("title")).lower()
        if "ready" in title and "print" in title:
            return _clean(g.get("id"))
    for g in groups:
        if "print" in _clean(g.get("title")).lower():
            return _clean(g.get("id"))
    return _clean(groups[0].get("id")) if groups else None


def monday_find_item_by_name(name: str) -> dict[str, Any] | None:
    cursor = None
    while True:
        q = """query($board:[ID!]!, $cursor:String) {
          boards(ids:$board) {
            items_page(limit:500, cursor:$cursor) {
              cursor
              items {
                id
                name
                group { id }
                column_values(ids:["files__1"]) { id text value }
              }
            }
          }
        }"""
        data = monday_query(q, {"board": [PRINT_STATUS_BOARD_ID], "cursor": cursor})
        page = (((data.get("boards") or [{}])[0]).get("items_page") or {})
        for item in page.get("items") or []:
            if _clean(item.get("name")) == name:
                return item
        cursor = page.get("cursor")
        if not cursor:
            return None


def monday_create_item(name: str, group_id: str | None) -> dict[str, Any]:
    if group_id:
        q = """mutation($board:ID!, $group:String!, $name:String!) {
          create_item(board_id:$board, group_id:$group, item_name:$name) { id name }
        }"""
        return monday_query(q, {"board": PRINT_STATUS_BOARD_ID, "group": group_id, "name": name})["create_item"]
    q = """mutation($board:ID!, $name:String!) {
      create_item(board_id:$board, item_name:$name) { id name }
    }"""
    return monday_query(q, {"board": PRINT_STATUS_BOARD_ID, "name": name})["create_item"]


def monday_item_has_file(item: dict[str, Any]) -> bool:
    for cv in item.get("column_values") or []:
        if cv.get("id") != PRINT_PACKING_LIST_COLUMN_ID:
            continue
        value = _clean(cv.get("value"))
        text = _clean(cv.get("text"))
        if value not in ("", "null", "[]", "{}") or text:
            return True
    return False


def monday_upload_file(item_id: str, filename: str, pdf_bytes: bytes) -> None:
    token = _clean(os.environ.get("MONDAY_API_TOKEN"))
    query = """mutation($item:ID!, $column:String!, $file:File!) {
      add_file_to_column(item_id:$item, column_id:$column, file:$file) { id }
    }"""
    operations = {
        "query": query,
        "variables": {
            "item": str(item_id),
            "column": PRINT_PACKING_LIST_COLUMN_ID,
            "file": None,
        },
    }
    files = {
        "variables[file]": (filename, pdf_bytes, "application/pdf"),
    }
    r = requests.post(
        MONDAY_FILE_GQL,
        headers={"Authorization": token},
        data={"query": query, "variables": json.dumps(operations["variables"])},
        files=files,
        timeout=90,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"Monday file upload failed ({r.status_code}): {r.text[:400]}")
    body = r.json()
    if body.get("errors"):
        raise RuntimeError("; ".join(_clean(e.get("message") or e) for e in body["errors"]))


def tb_find_work_order(token: str, wo: str) -> dict[str, Any]:
    q = """query SearchWorkOrders($first:Int!, $search:String) {
      workOrders(first:$first, search:$search) {
        nodes { id installerName onfleetRoutePlanName }
      }
    }"""
    nodes = (tb_query(token, q, {"first": 50, "search": wo}).get("workOrders") or {}).get("nodes") or []
    exact = [n for n in nodes if _clean(n.get("onfleetRoutePlanName")) == wo]
    if exact:
        return exact[0]
    if len(nodes) == 1:
        return nodes[0]
    raise RuntimeError(f"Terraboost work order '{wo}' was not uniquely matched")


def tb_get_work_order(token: str, work_order_id: int) -> dict[str, Any]:
    q = """query GetWorkOrderById($id:Int!) {
      workOrderById(id:$id) {
        id
        installerName
        onfleetRoutePlanName
        statusId
        workOrderDueDateLocal
        routePlanTimezone
        workOrderTasks {
          id
          name
          kioskId
          campaignId
          notes
          taskTypeId
          workOrderTaskSequence
          campaign {
            id
            orderNumber
            name
            customerCampaigns {
              customer { customerType { typeName } }
            }
          }
          campaignKiosk {
            boosted
            printCollection { collectionName }
          }
          kiosk {
            importKioskId
            isDigital
            kioskLocation { typeName }
            venueId
            venue {
              venueName
              address1
              address2
              city
              state
              zip
            }
          }
        }
      }
    }"""
    wo = tb_query(token, q, {"id": int(work_order_id)}).get("workOrderById")
    if not wo:
        raise RuntimeError(f"Terraboost work order id {work_order_id} not found")
    return wo


def tb_record_type_names(token: str) -> dict[int, str]:
    q = """query {
      recordTypes(where:{active:{eq:true}}) { id typeName }
    }"""
    rows = tb_query(token, q).get("recordTypes") or []
    out: dict[int, str] = {}
    for row in rows:
        try:
            out[int(row.get("id"))] = _clean(row.get("typeName"))
        except Exception:
            pass
    return out


def _fmt_address(venue: dict[str, Any]) -> str:
    line1 = ", ".join(x for x in [_clean(venue.get("address1")), _clean(venue.get("address2"))] if x)
    city_state = ", ".join(x for x in [_clean(venue.get("city")), _clean(venue.get("state"))] if x)
    tail = " ".join(x for x in [city_state, _clean(venue.get("zip"))] if x)
    return ", ".join(x for x in [line1, tail] if x)


def build_packing_pdf(wo: dict[str, Any], type_names: dict[int, str]) -> tuple[str, bytes]:
    route_name = _clean(wo.get("onfleetRoutePlanName")) or f"WO-{wo.get('id')}"
    installer = _clean(wo.get("installerName")) or "Unassigned"
    tasks = list(wo.get("workOrderTasks") or [])
    tasks.sort(key=lambda t: (t.get("workOrderTaskSequence") is None, t.get("workOrderTaskSequence") or 999999, t.get("id") or 0))

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=landscape(letter),
        leftMargin=0.25 * inch,
        rightMargin=0.25 * inch,
        topMargin=0.25 * inch,
        bottomMargin=0.25 * inch,
        title=route_name,
    )
    styles = getSampleStyleSheet()
    story = [
        Paragraph(f"<b>{installer}</b>", styles["Title"]),
        Paragraph(f"WO#: {route_name}", styles["Normal"]),
        Spacer(1, 8),
    ]

    headers = ["Kiosk ID", "Notes", "Boosted/Standard", "SIO", "Task Type", "Venue Name", "Client Company", "Allocated Resources", "Customer Type", "State"]
    rows: list[list[Any]] = [headers]

    for t in tasks:
        kiosk = t.get("kiosk") or {}
        venue = kiosk.get("venue") or {}
        campaign = t.get("campaign") or {}
        campaign_kiosk = t.get("campaignKiosk") or {}
        customer_type = ""
        ccs = campaign.get("customerCampaigns") or []
        if ccs:
            customer_type = _clean((((ccs[0] or {}).get("customer") or {}).get("customerType") or {}).get("typeName"))
        task_type = type_names.get(int(t.get("taskTypeId") or 0), "")
        rows.append([
            _clean(kiosk.get("importKioskId")),
            _clean(t.get("notes")),
            "Boosted" if campaign_kiosk.get("boosted") else "Standard",
            _clean(campaign.get("orderNumber")) or "Default",
            task_type,
            _clean(venue.get("venueName")),
            _clean(campaign.get("name")) or "Default",
            _clean(((campaign_kiosk.get("printCollection") or {}).get("collectionName"))),
            customer_type,
            _clean(venue.get("state")),
        ])

    table = Table(
        rows,
        repeatRows=1,
        colWidths=[0.65*inch,1.0*inch,0.8*inch,0.72*inch,0.85*inch,0.95*inch,1.55*inch,1.0*inch,0.8*inch,0.45*inch],
    )
    style_cmds = [
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTSIZE", (0,0), (-1,0), 8),
        ("FONTSIZE", (0,1), (-1,-1), 8),
        ("GRID", (0,0), (-1,-1), 0.35, colors.lightgrey),
        ("VALIGN", (0,0), (-1,-1), "TOP"),
        ("LEFTPADDING", (0,0), (-1,-1), 3),
        ("RIGHTPADDING", (0,0), (-1,-1), 3),
        ("TOPPADDING", (0,0), (-1,-1), 3),
        ("BOTTOMPADDING", (0,0), (-1,-1), 3),
    ]
    for i, t in enumerate(tasks, start=1):
        if (t.get("campaignKiosk") or {}).get("boosted"):
            style_cmds.append(("BACKGROUND", (0,i), (-1,i), colors.HexColor("#DDEEFF")))
    table.setStyle(TableStyle(style_cmds))
    story.append(table)
    doc.build(story)

    safe = re.sub(r"[^A-Za-z0-9._ -]+", "_", route_name).strip() or f"WO-{wo.get('id')}"
    return f"{safe}.pdf", buf.getvalue()


def sync_one(route: dict[str, Any], board: dict[str, Any], group_id: str | None, token: str, type_names: dict[int, str]) -> dict[str, Any]:
    wo_name = _clean(route.get("wo"))
    if not wo_name:
        return {"status": "skipped", "reason": "blank WO"}

    item = monday_find_item_by_name(wo_name)
    created = False
    if item is None:
        item = monday_create_item(wo_name, group_id)
        item["column_values"] = []
        created = True

    if monday_item_has_file(item):
        return {"wo": wo_name, "item_id": item.get("id"), "created": created, "status": "already_has_pdf"}

    match = tb_find_work_order(token, wo_name)
    detail = tb_get_work_order(token, int(match["id"]))
    filename, pdf_bytes = build_packing_pdf(detail, type_names)
    monday_upload_file(str(item["id"]), filename, pdf_bytes)
    return {
        "wo": wo_name,
        "item_id": item.get("id"),
        "created": created,
        "status": "uploaded",
        "filename": filename,
        "bytes": len(pdf_bytes),
    }


def main() -> None:
    result: dict[str, Any] = {
        "board_id": PRINT_STATUS_BOARD_ID,
        "pod": POD_FILTER,
        "not_before": NOT_BEFORE or None,
        "test_wo": TEST_WO or None,
        "processed": [],
        "errors": [],
    }
    board = monday_board_meta()
    group_id = choose_target_group(board)
    result["board_name"] = board.get("name")
    result["group_id"] = group_id

    routes = get_orange_accepted_routes()
    result["candidate_count"] = len(routes)
    if not routes:
        print("PACKING_SYNC=" + json.dumps(result, separators=(",", ":"), default=str), flush=True)
        return

    token = tb_login()
    type_names = tb_record_type_names(token)

    for route in routes:
        try:
            result["processed"].append(sync_one(route, board, group_id, token, type_names))
        except Exception as exc:
            result["errors"].append({"wo": _clean(route.get("wo")), "error": str(exc)[:500]})

    print("PACKING_SYNC=" + json.dumps(result, separators=(",", ":"), default=str), flush=True)


if __name__ == "__main__":
    main()

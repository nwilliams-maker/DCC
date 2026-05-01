"""
packing_slip.py — Renders a "Packing Slip" print button per accepted route.

The PDF is generated client-side using jsPDF and the exact same renderPackingSlipInto
function from index_45.html, embedded into a Streamlit components.v1.html iframe.
The button is sized small (~36px) and styled to match the .btn .btn-mini class from
the original portal so it slots in next to the existing route content without
visual disruption.

Usage in tactical_workspace_master_rw.py:

    from packing_slip import render_packing_slip_button

    # Inside each accepted-route expander, after the existing Route Summary HTML:
    render_packing_slip_button(
        cluster=c,                  # the accepted cluster dict
        pod_name=pod_name,          # 'Blue' | 'Green' | 'Orange' | 'Purple' | 'Red'
        key=cluster_hash,           # any unique string for this route
    )

DATA NOTES — what the Streamlit data has vs. what index_45.html had:
  Available  : task_type, venue_name, venue_id, location_in_venue,
               boosted_standard, client_company, full address, state, is_digital
  Missing    : SIO# (special-instruction-order), notes/artFile, payPerTask,
               assignedDate, customerType — these get sensible fallbacks below.
  Derived    : customerType (from "national" appearing in client_company),
               kioskType ('Digital' if is_digital else 'Premium'),
               isInstall (strict 'kiosk install' match),
               stopNumber (index in cluster['data']).
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

import streamlit.components.v1 as components


# ---------------------------------------------------------------------------
# Campaign string cleanup — strips date ranges and stray single dates from
# the campaign name before it lands on the slip. Examples:
#
#   "Dairy Farmers - National Campaign 04/13/2026-12/31/2026 - National"
#     → "Dairy Farmers - National Campaign - National"
#
#   "Scorch LLC (PG&E) - National Campaign (03/30/2026-06/30/2026)"
#     → "Scorch LLC (PG&E) - National Campaign"
#
#   "Some Campaign 11/15/2026"
#     → "Some Campaign"
#
# Strips MM/DD/YYYY and MM/DD/YY (with or without leading zeros), as a single
# date or a hyphen-separated range, with optional surrounding parens. Doesn't
# touch year-only tokens like "2026 Tejava" — slashes are required.
# ---------------------------------------------------------------------------
_DATE_PAREN_RE  = re.compile(r"\s*\(\s*\d{1,2}/\d{1,2}/\d{2,4}(?:\s*-\s*\d{1,2}/\d{1,2}/\d{2,4})?\s*\)")
_DATE_RANGE_RE  = re.compile(r"\s*\d{1,2}/\d{1,2}/\d{2,4}\s*-\s*\d{1,2}/\d{1,2}/\d{2,4}")
_SINGLE_DATE_RE = re.compile(r"\s*\d{1,2}/\d{1,2}/\d{2,4}")
# Per dispatcher spec: strip the literal phrase "National Campaign" from
# campaign strings — it's redundant since the National Summary card and the
# trailing " - National" customer type already convey the same information.
# Match is case-insensitive; surrounding whitespace and dashes are tidied up
# in the cleanup pass below.
_NATIONAL_CAMP_RE = re.compile(r"\bnational\s+campaign\b", re.IGNORECASE)


def _strip_campaign_dates(s: str) -> str:
    """Remove date ranges / single dates / 'National Campaign' boilerplate
    from a campaign string and tidy up any leftover whitespace + orphan
    dashes."""
    if not s:
        return s
    out = _DATE_PAREN_RE.sub(" ", str(s))
    out = _DATE_RANGE_RE.sub(" ", out)
    out = _SINGLE_DATE_RE.sub(" ", out)
    out = _NATIONAL_CAMP_RE.sub(" ", out)
    # Collapse runs of whitespace
    out = re.sub(r"\s+", " ", out)
    # Collapse orphan double-dashes left behind ("Campaign  - National" → already fine,
    # but "Campaign -  - National" → "Campaign - National")
    out = re.sub(r"\s*-\s*-\s*", " - ", out)
    # Trim leading/trailing whitespace + stray dashes
    out = re.sub(r"^[\s\-]+|[\s\-]+$", "", out)
    return out


# ---------------------------------------------------------------------------
# JS — extracted verbatim from index_45.html so output matches byte-for-byte.
# Helpers (esc, groupBy, isInstallType, stripCampaignDates, normalizeKioskType,
# extractArtFile), then fitText, then the full packing-slip renderer.
# ---------------------------------------------------------------------------
_JS_HELPERS = r"""
  // No-op toast — the iframe has no toastHost element. Errors fall through to console.
  function toast(msg, type) { try { console.log('[' + (type || 'info') + '] ' + msg); } catch(_) {} }

  function esc(s) { return String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
  function groupBy(arr, key) {
    const m = new Map();
    for (const r of arr) {
      const k = typeof key === 'function' ? key(r) : (r[key] ?? '—');
      if (!m.has(k)) m.set(k, []);
      m.get(k).push(r);
    }
    return m;
  }

  function isInstallType(t) { return String(t || '').trim().toLowerCase() === 'kiosk install'; }

  function stripCampaignDates(s) {
    if (!s) return s;
    let out = String(s);
    out = out.replace(/\s*\(\s*\d{1,2}\/\d{1,2}\/\d{2,4}\s*-\s*\d{1,2}\/\d{1,2}\/\d{2,4}\s*\)/g, '');
    out = out.replace(/\s+\d{1,2}\/\d{1,2}\/\d{2,4}\s*-\s*\d{1,2}\/\d{1,2}\/\d{2,4}\s*$/g, '');
    out = out.replace(/\s+\d{1,2}\/\d{1,2}\/\d{2,4}\s*$/g, '');
    const months = '(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)';
    let prev = null;
    let safety = 6;
    while (prev !== out && safety-- > 0) {
      prev = out;
      out = out.replace(new RegExp(`\\s*[-,]\\s*(?:Renewal|Renew),?\\s+${months}\\s+\\d{2,4}\\s*[,.]?\\s*$`, 'i'), '');
      out = out.replace(new RegExp(`\\s*[-,]\\s*${months}\\s+\\d{2,4}\\s*[,.]?\\s*$`, 'i'), '');
      out = out.replace(new RegExp(`\\s+(?:Renewal|Renew),?\\s+${months}\\s+\\d{2,4}\\s*[,.]?\\s*$`, 'i'), '');
      out = out.replace(new RegExp(`\\s+${months}\\s+\\d{2,4}\\s*[,.]?\\s*$`, 'i'), '');
      out = out.replace(/\s*[-,]\s*$/, '');
    }
    return out.trim();
  }

  function normalizeKioskType(raw) {
    if (raw == null) return 'Mini';
    let s = String(raw).trim();
    if (!s || /^(n\/a|null|—)$/i.test(s)) return 'Mini';
    if (/fry'?s\s*marketplace/i.test(s)) return 'Mini';
    s = s.replace(/[_\s]+/g, ' ').replace(/Premiun/gi, 'Premium').trim();
    if (/^lux(ury)?$/i.test(s)) return 'Lux';
    let titled = s.split(' ').map(w => w ? w[0].toUpperCase() + w.slice(1).toLowerCase() : '').join(' ');
    if (titled === 'Premium') return '';
    titled = titled.replace(/^Premium\s+/, '');
    return titled;
  }

  function extractArtFile(notes) {
    if (!notes) return '';
    const lines = String(notes).split(/\r?\n/).map(l => l.trim()).filter(Boolean);
    const keep = lines.filter(l => l.includes('_') && !/[.!?]\s*$/.test(l));
    return keep.join(' • ');
  }
"""


def _load_js_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


# Loaded lazily so this module can be imported without the .js files being co-located.
# In production we'll inline the JS into this file directly (see _PACKING_JS below).
_PACKING_JS: Optional[str] = None


def _packing_js() -> str:
    """Returns the full packing-slip JavaScript (helpers + fitText + renderer)."""
    global _PACKING_JS
    if _PACKING_JS is not None:
        return _PACKING_JS
    # Inline body is appended below by the build step.
    return _PACKING_JS_INLINE


# ---------------------------------------------------------------------------
# CLUSTER → ROW MAPPING
# ---------------------------------------------------------------------------
def _is_install(task_type: str) -> bool:
    """Strict match — only literal 'Kiosk Install' counts. Mirrors isInstallType in JS."""
    return str(task_type or "").strip().lower() == "kiosk install"


def _derive_customer_type(client_company: str, task_type: str) -> str:
    """Heuristic for the National vs Local suffix shown in the Client/Campaign
    column.

      - "default" anywhere in the campaign → empty (suffix dropped — these rows
        live in their own Defaults bucket and the National/Local label is
        meaningless for them).
      - "national" anywhere → "National"
      - everything else → "Local"

    The JS only checks for === 'national' vs everything else, so casing is
    irrelevant for the bucketing decision."""
    s = (client_company or "").lower()
    if "default" in s:
        return ""
    if "national" in s:
        return "National"
    return "Local"


def _map_cluster_to_rows(cluster: Dict[str, Any], pod_name: str) -> List[Dict[str, Any]]:
    """Convert a Streamlit cluster dict into the row format renderPackingSlipInto expects.

    Each task in cluster['data'] becomes one row. Fields that aren't captured during
    the OnFleet ingest (sio, notes, artFile, nationalCampName, payPerTask) are left
    blank — the JS renderer falls through gracefully when they're missing.
    """
    contractor = cluster.get("contractor_name") or "Unassigned"
    data = cluster.get("data") or []

    rows: List[Dict[str, Any]] = []
    for idx, t in enumerate(data, start=1):
        task_type = str(t.get("task_type", "") or "")
        # Clean the campaign string of date ranges before it lands on the slip.
        # See _strip_campaign_dates above for examples — turns
        #   "Dairy Farmers - National Campaign 04/13/2026-12/31/2026 - National"
        # into
        #   "Dairy Farmers - National Campaign - National".
        # The cleaned string is used for BOTH the displayed campaign and the
        # National/Local customer-type detection (the keyword "National" is
        # preserved by the strip).
        # Detect National vs Local on the RAW campaign string BEFORE stripping —
        # otherwise campaigns like "Acme - National Campaign" become "Acme" after
        # the "National Campaign" cleanup, lose the only "national" token, and
        # get miscounted as Local. The cleaned string is still used for display.
        raw_client_company = str(t.get("client_company", "") or "")
        client_company = _strip_campaign_dates(raw_client_company)
        is_digital = bool(t.get("is_digital", False))

        # Campaign-driven Default override: when task_type is "New Ad" but the
        # campaign string contains "default" (e.g. "Default", "Default -
        # Default", "Default - Default - Local"), flip the task type to
        # "Default". A "New Ad" with no real campaign IS a default ad — the
        # contractor only needs to know "default ad goes here."
        # Other task types (Remove Magnet, Photo Retake, Continuity, Kiosk
        # Install, etc.) stay as-is — those are physical actions the
        # contractor still has to perform regardless of what's in the slot.
        # Match is case-insensitive substring; safe because no real client
        # company name contains the word "default".
        if (task_type.strip().lower() == "new ad"
                and "default" in client_company.lower()):
            task_type = "Default"

        rows.append({
            # Identity
            "worker": contractor,
            "route": cluster.get("wo") or contractor,
            "pod": pod_name or "",
            # Stop / location
            "stopNumber": idx,
            "venue": t.get("venue_name", "") or "",
            "address": t.get("full", "") or "",
            "stateCode": (t.get("state", "") or "").strip().upper(),
            # Kiosk ID column is sourced from the OnFleet "kioskId" custom
            # field — distinct from the "venueId" custom field that the FN
            # mass-upload generator uses. Falls back to venue_id ONLY when a
            # task pre-dates the kioskId capture (data ingested before that
            # field was added). New data should always populate kiosk_id.
            "kiosk": t.get("kiosk_id", "") or t.get("venue_id", "") or "",
            "kioskLoc": t.get("location_in_venue", "") or "",
            # Kiosk type — Streamlit doesn't capture a kioskType custom field, so derive
            # from is_digital. Empty string ('Premium') is the default for non-digital,
            # which the JS normalizeKioskType drops from the display.
            "kioskType": "Digital" if is_digital else "",
            # Task / campaign / art
            "type": task_type,
            "boost": t.get("boosted_standard", "") or "",
            "campaign": client_company,
            "clientCompany": client_company,
            # Use raw (pre-strip) campaign string for National/Local detection.
            "customerType": _derive_customer_type(raw_client_company or client_company, task_type),
            "isInstall": _is_install(task_type),
            # ArtFile — extracted by the Streamlit app from each task's OnFleet
            # notes/Task Details (free-text). Surfaces dimmed under the campaign
            # name in the clientCol cell of Route Details + as the Allocated Art
            # column value in Locals.
            "artFile": str(t.get("art_file", "") or ""),
            # Other fields not captured during ingest — leave blank, JS handles fallbacks
            "sio": "",
            "notes": "",
            "nationalCampName": "",
        })
    return rows


# ---------------------------------------------------------------------------
# COMPONENT HTML
# ---------------------------------------------------------------------------
_BUTTON_CSS = """
<style>
  .ps-wrap { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
             margin: 0; padding: 0; }
  .ps-btn {
    display: inline-flex; align-items: center; justify-content: center;
    gap: 6px; padding: 6px 11px;
    border-radius: 6px;
    font-size: 11.5px; font-weight: 600;
    border: 1px solid #e2e8f0;
    background: #ffffff; color: #0c0a09;
    cursor: pointer; transition: all .15s ease;
    line-height: 1; white-space: nowrap;
  }
  .ps-btn:hover { border-color: #cbd5e1; background: #f8fafc; }
  .ps-btn:active { transform: translateY(1px); }
  .ps-btn:disabled { opacity: 0.5; cursor: progress; }
</style>
"""


def render_packing_slip_button(
    cluster: Dict[str, Any],
    pod_name: str,
    key: str,
    *,
    label: str = "📄 Packing Slip",
    height: int = 44,
) -> None:
    """Render a single 'Packing Slip' button bound to one accepted route.

    Args:
        cluster:   The accepted-route cluster dict (must have 'data', 'wo',
                   'contractor_name'). Same shape as `c` in the t_acc loop.
        pod_name:  'Blue' | 'Green' | 'Orange' | 'Purple' | 'Red' — used for the
                   coloured "<Pod> Pod" label on the slip header. Pass '' for none.
        key:       Unique string per route (cluster_hash works). Used to namespace
                   the click handler so multiple buttons on the same page don't
                   collide.
        label:     Button text. Defaults to "📄 Packing Slip".
        height:    Iframe height in px. 44 is enough for the button + its hover ring.
    """
    rows = _map_cluster_to_rows(cluster, pod_name or "")
    wo_name = cluster.get("wo") or cluster.get("contractor_name") or "Work Order"

    # Sanitize key for use as an HTML id attribute (no spaces / quotes / colons).
    btn_id = "ps-btn-" + "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in str(key))

    rows_json = json.dumps(rows)
    wo_name_json = json.dumps(wo_name)
    pod_name_json = json.dumps(pod_name or "")
    btn_id_json = json.dumps(btn_id)
    label_html = label.replace("<", "&lt;").replace(">", "&gt;")

    html = (
        _BUTTON_CSS
        + '<div class="ps-wrap">'
        + f'<button id="{btn_id}" class="ps-btn" type="button">{label_html}</button>'
        + '</div>'
        # jsPDF — pinned. ~250KB, browser-cached after first load.
        + '<script src="https://cdn.jsdelivr.net/npm/jspdf@2.5.2/dist/jspdf.umd.min.js"></script>'
        + '<script>(function(){\n'
        + f'const ROWS = {rows_json};\n'
        + f'const WO_NAME = {wo_name_json};\n'
        + f'const POD_NAME = {pod_name_json};\n'
        + f'const BTN_ID = {btn_id_json};\n'
        + _JS_HELPERS
        + _packing_js()
        + r"""
  // Click → build doc → save
  function _onClick(ev) {
    ev.preventDefault();
    const btn = document.getElementById(BTN_ID);
    if (!btn) return;
    btn.disabled = true;
    const oldText = btn.textContent;
    btn.textContent = '⏳ Generating…';
    try {
      const opts = POD_NAME ? { pod: POD_NAME } : {};
      const doc = buildPackingSlipDoc(WO_NAME, ROWS, opts);
      if (!doc) { btn.disabled = false; btn.textContent = oldText; return; }
      const date = new Date().toISOString().split('T')[0];
      const fname = String(WO_NAME).replace(/[^\w-]+/g, '_') + '_PackingSlip_' + date + '.pdf';
      savePdf(doc, fname);
    } catch (e) {
      console.error('Packing slip error:', e);
      alert('Packing slip failed: ' + e.message);
    } finally {
      btn.disabled = false;
      btn.textContent = oldText;
    }
  }

  function _bind() {
    const btn = document.getElementById(BTN_ID);
    if (btn) btn.addEventListener('click', _onClick);
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', _bind);
  else _bind();
})();</script>"""
    )

    components.html(html, height=height, scrolling=False)


# ---------------------------------------------------------------------------
# Inline JS — appended at module-build time. See packing_slip_builder.py.
# ---------------------------------------------------------------------------
_PACKING_JS_INLINE = r"""
  function fitText(doc, text, maxW) {
    if (text == null) return '';
    const s = String(text).replace(/\u00A0/g, ' ').trim();
    if (!s || s === 'N/A') return '';
    if (doc.getTextWidth(s) <= maxW) return s;
    // binary search for largest prefix that still fits with '…'
    let lo = 0, hi = s.length;
    while (lo < hi) {
      const mid = Math.floor((lo + hi + 1) / 2);
      if (doc.getTextWidth(s.slice(0, mid) + '…') <= maxW) lo = mid;
      else hi = mid - 1;
    }
    return s.slice(0, lo).trimEnd() + '…';
  }
  function savePdf(doc, filename) {
    if (!doc) return;
    try { doc.save(filename); toast('PDF downloaded', 'success'); }
    catch (e) { toast(`PDF save failed: ${e.message}`, 'error'); }
  }

  // ============ PACKING SLIP (warehouse-style — modeled on the legacy template) ============
  // Renders Worker name + WO# + Dispatcher, then 4 summary tables (Kiosk / Defaults / National
  // / Incorrect Artwork), then a wide details table.

  function buildPackingSlipDoc(woName, rows, opts = {}) {
    const jsPDFCtor = (window.jspdf && window.jspdf.jsPDF) || window.jsPDF;
    if (!jsPDFCtor) { toast('PDF library not loaded', 'error'); return null; }
    if (!rows || !rows.length) { toast('Nothing to print', 'warn'); return null; }

    const doc = new jsPDFCtor({ unit: 'pt', format: 'letter', orientation: 'landscape', compress: true });
    return renderPackingSlipInto(doc, woName, rows, opts);
  }

  function appendPackingSlipPage(existingDoc, woName, rows, opts = {}) {
    const jsPDFCtor = (window.jspdf && window.jspdf.jsPDF) || window.jsPDF;
    if (!jsPDFCtor) { toast('PDF library not loaded', 'error'); return null; }
    if (!rows || !rows.length) return existingDoc;
    // For continuation, add page in same orientation as the existing doc
    const doc = existingDoc || new jsPDFCtor({ unit: 'pt', format: 'letter', orientation: 'landscape', compress: true });
    return renderPackingSlipInto(doc, woName, rows, opts);
  }

  function renderPackingSlipInto(doc, woName, rows, opts) {
    const W = doc.internal.pageSize.width;
    const H = doc.internal.pageSize.height;
    const M = 36; // 0.5"
    const CONTENT_W = W - 2 * M;

    // Tasks that don't need a warehouse pull (continuity = artwork already in place;
    // photo = field-only documentation tasks; Digital Defaults = Premium Digital kiosks
    // running default content, no paper artwork to pull). They still appear in Route
    // Details so the worker has the full driving sequence, but they're rendered DIMMED
    // there and excluded from every "things to pack" surface (summary cards, Locals
    // table, Total Tasks count). The Digital Default check is inlined here because the
    // named `isDigitalDefault` predicate is defined later in this function.
    //
    // The Digital Default rule requires BOTH a default classification (sio=Default/0,
    // taskType=Default, Pull Down, etc.) AND a Premium Digital kiosk type without
    // "Magnet" in the task type. New Ad / Digital Service / etc. tasks on Digital
    // kiosks remain packable — they're real installs even if the panel is digital.
    const isDigitalDefaultInline = r => {
      const sio = (r.sio || '').toLowerCase();
      const tt = (r.type || '').toLowerCase();
      const boost = (r.boost || '').toLowerCase();
      const cn = (r.campaign || '').toLowerCase();
      const kt = (r.kioskType || '').toLowerCase();
      const isDef = sio === 'default' || sio === '0'
          || boost === 'default ad' || boost === 'default'
          || tt === 'default'
          || /\bpull\s*down\b/.test(tt) || /\bpull\s*down\b/.test(cn)
          || /\breplace\s+with\s+default\b/.test(cn);
      return isDef && kt === 'digital' && !/magnet/.test(tt);
    };
    const isPackable = r => {
      const tt = r.type || '';
      if (/\bcontinuity\b/i.test(tt)) return false;
      if (/\bphoto\b/i.test(tt)) return false;
      if (isDigitalDefaultInline(r)) return false;
      return true;
    };
    const allRows = rows;
    const packableRows = rows.filter(isPackable);
    if (!packableRows.length) {
      // Edge case: WO has zero packable tasks. Render a friendly empty slip.
      doc.setFont('helvetica', 'normal');
      doc.setFontSize(11);
      doc.setTextColor(120, 120, 120);
      doc.text(`${woName} — no packable tasks (all are continuity or photo).`, M, M + 30);
      return doc;
    }
    // From here on, `rows` is the packable subset. Route Details rebinds to allRows below.
    rows = packableRows;

    const worker = [...new Set(rows.map(r => r.worker))][0] || 'Unassigned';
    const today = new Date().toLocaleDateString();

    // Pod name comes from the rows themselves (each task has r.pod from STATE_TO_POD).
    // The "Dispatcher" line on the slip shows "{Pod} Pod" rendered in the pod's color.
    const podName = (() => {
      const pods = [...new Set(rows.map(r => r.pod).filter(Boolean))];
      return pods.length === 1 ? pods[0] : (pods.length > 1 ? 'Mixed' : null);
    })();
    const POD_COLORS = {
      Blue:   [37, 99, 235],
      Green:  [22, 163, 74],
      Orange: [234, 88, 12],
      Purple: [147, 51, 234],
      Red:    [220, 38, 38],
      Mixed:  [107, 43, 201],
    };
    const dispatcherLabel = podName ? `${podName} Pod` : (opts.dispatcher || 'Dispatch');
    const dispatcherColor = podName ? POD_COLORS[podName] : [40, 40, 40];

    // Categorize
    const isDefault   = r => {
      const sio = (r.sio || '').toLowerCase();
      const tt  = (r.type || '').toLowerCase();
      const boost = (r.boost || '').toLowerCase();
      const cn = (r.campaign || '').toLowerCase();
      // A task is "Default" if any of these are true:
      //   1. SIO is literally "default" or the "0" sentinel (both mean no specific SIO order)
      //   2. Boost (boostedStandard) is "default" or "default ad"
      //   3. Task type is exactly "default" (the bare-default task type)
      //   4. Task type contains "pull down" — actual pull-down operation
      //   5. Campaign name contains "pull down" or "replace with default" — captures
      //      the few edge cases where the specific task is a New Ad / Kiosk Install but
      //      the broader campaign is a pull-down / replace-with-default operation.
      //      The warehouse pulls default art for these regardless of the per-task type.
      return sio === 'default' || sio === '0'
          || boost === 'default ad' || boost === 'default'
          || tt === 'default'
          || /\bpull\s*down\b/.test(tt)
          || /\bpull\s*down\b/.test(cn)
          || /\breplace\s+with\s+default\b/.test(cn);
    };
    // Digital Defaults are tasks that are BOTH classified as Default (Pull Down, sio=Default,
    // taskType=Default, etc.) AND are on a Premium Digital kiosk where the task type does NOT
    // contain "Magnet" (Digital Ad With Magnet tasks ARE real pulls — they need the
    // physical magnet artwork). Per spec, Digital Defaults never appear in the Default
    // Summary because Digital screens don't pull paper artwork from the default pool.
    // They still appear in Route Details so the worker sees the stop, with the task type
    // relabeled as "Remove Magnet" and the row dimmed.
    const isDigitalDefault = r => {
      if (!isDefault(r)) return false;
      const kt = (r.kioskType || '').toLowerCase();
      const tt = (r.type || '').toLowerCase();
      return kt === 'digital' && !/magnet/.test(tt);
    };
    const isNational  = r => (r.customerType || '').toLowerCase() === 'national';
    const isIncorrect = r => /photo\s*retake|location\s+in\s+venue\s+incorrect|incorrect/i.test(r.type || '');

    // Bucket order matters:
    // Incorrect supersedes nothing now — incorrect tasks stay in their natural bucket
    // (Local / National / Default) but get highlighted red in the detail table.
    // Default supersedes National (a Pull Down on a National task is still "Default")
    // EXCEPT for Digital Defaults — those are stripped from every summary bucket since
    // they don't represent a paper-pull operation. They still appear in Route Details.
    const defaultRows  = rows.filter(r => isDefault(r) && !isDigitalDefault(r));
    const nationalRows = rows.filter(r => !isDefault(r) && isNational(r));
    // Locals = all non-default/non-national tasks
    const localRows    = rows.filter(r => !isDefault(r) && !isNational(r));
    // Kiosk Summary specifically counts only physical Kiosk Install tasks
    // (these can come from any bucket — local OR national install jobs both go here)
    const kioskInstallRows = rows.filter(r => r.isInstall);

    // ---- Kiosk Summary — venue + count, sorted by venue, INSTALLS ONLY ----
    const kioskByVenue = aggregate(kioskInstallRows, r => r.venue || '—')
      .sort((a, b) => String(a.label).localeCompare(String(b.label)));

    // ---- Defaults — sort by venue, group with count ----
    const defaultByVenue = aggregate(defaultRows, r => r.venue || '—')
      .sort((a, b) => String(a.label).localeCompare(String(b.label)));

    // ---- National — sort by Art (boost), include art type for every campaign ----
    // For the National Summary specifically, we want the CONCISE national campaign reference
    // (e.g. "Pull Down - ATT", "AbbVi - Refresh - 25125693") — that lives in nationalCampName.
    // The verbose campaignName field on national tasks often contains operational instruction
    // text like "Pull Down ATT - Gulf Stream - REPLACE with Default" which isn't a campaign
    // name at all, so we don't use it as the summary label here. Per-row detail tables
    // (Locals, Route Details) still use campaignName since that's what works for locals.
    const nationalLabel = r => {
      const ncn = (r.nationalCampName || '').trim();
      // Treat literal "Local" placeholder and N/A as missing
      if (ncn && !/^(local|n\/a|null|—)$/i.test(ncn)) return ncn;
      return (r.campaign || '').trim() || '—';
    };
    const nationalByArt = aggregateBy(
      nationalRows,
      r => (r.boost || '—') + '||' + (r.clientCompany || '—') + '||' + nationalLabel(r) + '||' + (r.kioskType || ''),
      r => ({
        art: r.boost || '—',
        client: r.clientCompany || '—',
        campaign: nationalLabel(r),
        kioskType: r.kioskType || '',
        // Collect distinct allocated art file names across all tasks in this group. Same
        // campaign + same hardware almost always shares one art file, but it's worth
        // surfacing the rare case where production needs to pull two different files.
        artFiles: new Set(),
      })
    );
    // Populate the artFiles set for each grouped item by walking the source rows again.
    // (aggregateBy doesn't expose row-level access to the reducer; this second pass is
    // simple and the row count is small enough that performance doesn't matter here.)
    for (const r of nationalRows) {
      const key = (r.boost || '—') + '||' + (r.clientCompany || '—') + '||' + nationalLabel(r) + '||' + (r.kioskType || '');
      const af = (r.artFile || '').trim();
      if (!af) continue;
      const item = nationalByArt.find(it =>
        ((it.art || '—') + '||' + (it.client || '—') + '||' + it.campaign + '||' + (it.kioskType || '')) === key
      );
      if (item) item.artFiles.add(af);
    }
    nationalByArt.sort((a, b) => {
      const byArt = String(a.art).localeCompare(String(b.art));
      if (byArt !== 0) return byArt;
      return String(a.client).localeCompare(String(b.client));
    });

    function aggregate(arr, keyFn) {
      const m = new Map();
      for (const r of arr) m.set(keyFn(r), (m.get(keyFn(r)) || 0) + 1);
      return [...m.entries()].sort((a,b) => b[1] - a[1]).map(([k,v]) => ({ label: k, count: v }));
    }
    function aggregateBy(arr, keyFn, init) {
      const m = new Map();
      for (const r of arr) {
        const k = keyFn(r);
        if (!m.has(k)) m.set(k, { ...init(r), count: 0 });
        m.get(k).count++;
      }
      return [...m.values()].sort((a,b) => b.count - a.count);
    }

    // ============ HEADER ============
    let y = M;

    // Eyebrow
    doc.setFont('helvetica', 'bold');
    doc.setFontSize(8);
    doc.setTextColor(107, 43, 201);
    doc.setCharSpace(1.2);
    doc.text('TERRABOOST MEDIA  ·  PACKING SLIP', M, y + 8);
    doc.setCharSpace(0);

    // Right side: date
    doc.setFont('helvetica', 'normal');
    doc.setFontSize(8.5);
    doc.setTextColor(120, 120, 120);
    doc.text(today, W - M, y + 8, { align: 'right' });

    // ============ LEGEND (in header row, between eyebrow and date) ============
    // Compact horizontal key explaining the visual indicators production will see in
    // the row tables below. Sits in the same line as the eyebrow and date so it doesn't
    // consume any extra vertical space — production reads it once at the very top of
    // the slip, then has the visual reference for everything below.
    {
      const items = [
        { kind: 'fill',   color: [232, 218, 250], label: 'Boosted Ad' },
        { kind: 'accent', color: [22, 163, 74],   label: 'Kiosk Install' },
        { kind: 'fill',   color: [254, 242, 242], label: 'Photo Retake' },
        { kind: 'dim',                            label: 'Continuity · Photo · Remove Magnet' },
      ];
      const SWATCH_W = 11, SWATCH_H = 7, GAP = 11, ITEM_GAP = 4;
      doc.setFont('helvetica', 'normal');
      doc.setFontSize(7);
      const labelWidths = items.map(it => doc.getTextWidth(it.label));

      // Compute total width and center the strip horizontally between the page edges,
      // with a "KEY" eyebrow on the far left of the strip
      let totalW = 0;
      items.forEach((_, i) => { totalW += SWATCH_W + ITEM_GAP + labelWidths[i]; });
      totalW += GAP * (items.length - 1);

      const keyLabel = 'KEY';
      doc.setFont('helvetica', 'bold');
      doc.setFontSize(7);
      doc.setCharSpace(0.6);
      const keyW = doc.getTextWidth(keyLabel) + 7;
      doc.setCharSpace(0);

      // Determine the center of the gap between the eyebrow text on the left and the
      // date text on the right, so the legend sits neatly between them rather than
      // overlapping. We re-measure the eyebrow and date widths in their actual fonts.
      doc.setFont('helvetica', 'bold');
      doc.setFontSize(8);
      doc.setCharSpace(1.2);
      const eyebrowW = doc.getTextWidth('TERRABOOST MEDIA  ·  PACKING SLIP');
      doc.setCharSpace(0);
      doc.setFont('helvetica', 'normal');
      doc.setFontSize(8.5);
      const dateW = doc.getTextWidth(today);

      // Available range for the legend: from (M + eyebrowW + buffer) to (W - M - dateW - buffer).
      // Center the legend (KEY + items) inside that available range.
      const BUFFER = 14;
      const availLeft = M + eyebrowW + BUFFER;
      const availRight = W - M - dateW - BUFFER;
      const totalStripW = totalW + keyW;
      const startX = availLeft + ((availRight - availLeft) - totalStripW) / 2;
      const baseY = y + 8;     // align with eyebrow text baseline
      const swatchY = y + 2;   // swatches start a couple pts above text baseline

      // KEY eyebrow
      doc.setFont('helvetica', 'bold');
      doc.setFontSize(7);
      doc.setTextColor(140, 140, 140);
      doc.setCharSpace(0.6);
      doc.text(keyLabel, startX, baseY);
      doc.setCharSpace(0);

      let cursorX = startX + keyW;
      doc.setFont('helvetica', 'normal');
      doc.setFontSize(7);

      items.forEach((it, i) => {
        if (it.kind === 'fill') {
          doc.setFillColor(it.color[0], it.color[1], it.color[2]);
          doc.setDrawColor(220, 220, 220);
          doc.setLineWidth(0.3);
          doc.roundedRect(cursorX, swatchY, SWATCH_W, SWATCH_H, 1.5, 1.5, 'FD');
        } else if (it.kind === 'accent') {
          doc.setFillColor(245, 245, 245);
          doc.setDrawColor(220, 220, 220);
          doc.setLineWidth(0.3);
          doc.roundedRect(cursorX, swatchY, SWATCH_W, SWATCH_H, 1.5, 1.5, 'FD');
          doc.setFillColor(it.color[0], it.color[1], it.color[2]);
          doc.rect(cursorX, swatchY, 1.8, SWATCH_H, 'F');
        } else if (it.kind === 'dim') {
          doc.setFillColor(248, 248, 247);
          doc.setDrawColor(220, 220, 220);
          doc.setLineWidth(0.3);
          doc.roundedRect(cursorX, swatchY, SWATCH_W, SWATCH_H, 1.5, 1.5, 'FD');
          doc.setTextColor(185, 185, 185);
          doc.setFontSize(5.5);
          doc.text('Aa', cursorX + SWATCH_W / 2, swatchY + 5, { align: 'center' });
          doc.setFontSize(7);
        }
        cursorX += SWATCH_W + ITEM_GAP;
        doc.setTextColor(90, 90, 90);
        doc.text(it.label, cursorX, baseY);
        cursorX += labelWidths[i] + GAP;
      });
    }

    y += 22;

    // Worker name — large, the marquee element
    doc.setFont('times', 'italic');
    doc.setFontSize(34);
    doc.setTextColor(12, 10, 9);
    doc.text(worker, M, y + 26);
    y += 38;

    // Meta row: WO# + Dispatcher + task count, on one line with separators
    doc.setFont('helvetica', 'normal');
    doc.setFontSize(9);
    doc.setTextColor(120, 120, 120);

    const meta = [];
    meta.push({ label: 'WO#',         value: woName });
    meta.push({ label: 'DISPATCHER',  value: dispatcherLabel, color: dispatcherColor, bold: !!podName });
    meta.push({ label: 'TOTAL TASKS', value: String(rows.length) });

    let mx = M;
    for (let i = 0; i < meta.length; i++) {
      const m = meta[i];
      // Eyebrow label (always purple, small)
      doc.setFont('helvetica', 'bold');
      doc.setFontSize(7);
      doc.setTextColor(107, 43, 201);
      doc.setCharSpace(0.6);
      doc.text(m.label, mx, y + 2);
      doc.setCharSpace(0);
      // Value — color and weight optionally per-item
      doc.setFont('helvetica', m.bold ? 'bold' : 'normal');
      doc.setFontSize(10);
      const c = m.color || [40, 40, 40];
      doc.setTextColor(c[0], c[1], c[2]);
      doc.text(m.value, mx, y + 14);
      doc.setFont('helvetica', 'normal'); // reset
      const w = Math.max(doc.getTextWidth(m.value), doc.getTextWidth(m.label) + 6);
      mx += w + 28;
    }

    y += 26;

    // Divider rule
    doc.setDrawColor(232, 232, 230);
    doc.setLineWidth(0.6);
    doc.line(M, y, W - M, y);
    y += 18;

    // ============ SUMMARY CARDS ============
    // 3 cards across in a single row.
    const cardGap = 14;
    const cardW = (CONTENT_W - cardGap * 2) / 3;

    const cards = [
      {
        title: 'Kiosk Summary',
        subtitle: 'sorted by venue',
        accent: [22, 163, 74],   // green
        data: kioskByVenue,
        layout: 'count',
        empty: 'No kiosk installs',
      },
      {
        title: 'Defaults Summary',
        subtitle: 'sorted by venue',
        accent: [217, 119, 6],   // amber
        data: defaultByVenue,
        layout: 'count',
        empty: 'No default tasks',
      },
      {
        title: 'National Summary',
        subtitle: 'sorted by art',
        accent: [37, 99, 235],   // blue
        data: nationalByArt,
        layout: 'nationalByArt',
        empty: 'No national tasks',
      },
    ];

    // All cards in one row — track tallest so detail table follows below
    let maxBottom = y;
    for (let i = 0; i < cards.length; i++) {
      const x = M + i * (cardW + cardGap);
      const endY = drawSummaryCard(x, y, cardW, cards[i]);
      if (endY > maxBottom) maxBottom = endY;
    }
    y = maxBottom + cardGap;

    // ============ LOCALS TABLE ============
    // Sort by SIO ascending, with sensible handling for non-numeric values:
    //   1. Numeric SIOs first, sorted ascending (the common case — "25094763" < "26036421")
    //   2. Other text SIOs after the numbers, alphabetical
    //   3. Default sentinels (blank / NULL / N/A / 'Default' / '0') pushed to the end so
    //      they don't break up the numbered groups
    // Secondary sort by Kiosk ID so same-SIO rows always read in a consistent order.
    const sioSortKey = r => {
      const raw = String(r.sio || '').trim();
      if (!raw || /^(null|n\/a|—|default|0)$/i.test(raw)) return [3, '', ''];
      if (/^\d+$/.test(raw)) return [1, parseInt(raw, 10), ''];
      return [2, 0, raw.toLowerCase()];
    };
    const localsSorted = localRows.slice().sort((a, b) => {
      const ka = sioSortKey(a), kb = sioSortKey(b);
      if (ka[0] !== kb[0]) return ka[0] - kb[0];
      if (ka[1] !== kb[1]) return ka[1] - kb[1];
      if (ka[2] !== kb[2]) return ka[2] < kb[2] ? -1 : 1;
      // Tie-break by Kiosk ID so same-SIO rows are stable
      return String(a.kiosk || '').localeCompare(String(b.kiosk || ''));
    });

    if (localsSorted.length) {
      // Only surface the Art File column when there's actually data to show — otherwise
      // it's just a column of dashes wasting space.
      const anyArtFile = localsSorted.some(r => (r.artFile || '').trim());
      y = drawDetailsTable(
        y, localsSorted,
        'LOCALS',
        `${localsSorted.length} task${localsSorted.length === 1 ? '' : 's'} · sorted by SIO`,
        // localsLayout: true uses the warehouse-tuned column order
        // (Notes · Kiosk ID · SIO · Kiosk Type · Art · Task · Venue · Client/Campaign ·
        // Allocated Art · State). The Allocated Art column is always present in this
        // layout per spec, regardless of whether any rows have art files.
        // groupLocals: collapses runs of rows that share artwork-pull fields
        // (SIO + Campaign + Task Type + Kiosk Type + Notes + Allocated Art + State)
        // into a single master row + a small sub-line listing each kiosk and venue.
        { hideAddress: true, deemphasizeInstalls: true, useSioColumn: true, showArtFile: true, localsLayout: true, groupLocals: true }
      );
    }

    // ============ ROUTE DETAILS TABLE (every task) ============
    // OnFleet returns tasks in stop order within each route; we captured that as
    // r.stopNumber during ingest. Route Details lists every task in that order — INCLUDING
    // continuity and photo tasks, which are rendered dimmed so production knows to ignore
    // them but the worker still sees their full route.
    //
    // If the entire slip fits on one page (small WOs), keep Route Details inline below
    // Locals — saves a sheet of paper. Otherwise force it onto a fresh page so production
    // doesn't have to flip back and forth between summary and route info.
    const sortedRows = allRows.slice().sort((a, b) => (a.stopNumber || 0) - (b.stopNumber || 0));
    const omittedCount = sortedRows.length - sortedRows.filter(isPackable).length;
    const subtitle = omittedCount
      ? `${sortedRows.length} stop${sortedRows.length === 1 ? '' : 's'} · ${omittedCount} dimmed (continuity/photo) · sorted by stop #`
      : `${sortedRows.length} task${sortedRows.length === 1 ? '' : 's'} · sorted by stop #`;

    // Estimate the height Route Details needs:
    //   ~32pt for section title + subtitle
    //   +22pt for the column header row
    //   +22pt (or 30pt if any row has an artFile sub-line) per data row
    //   +6pt safety margin for the bottom separator line
    const ROW_H_EST = sortedRows.some(r => r && r.artFile && String(r.artFile).trim()) ? 30 : 22;
    const routeDetailsH = 32 + 22 + sortedRows.length * ROW_H_EST + 6;
    // Gap + divider rule that goes between Locals and Route Details when same-page.
    const SAME_PAGE_GAP = 32;
    const fits = (y + SAME_PAGE_GAP + routeDetailsH) <= (H - M);

    if (fits) {
      // Same page — give it some breathing room and a subtle divider rule
      y += 14;
      doc.setDrawColor(232, 232, 230);
      doc.setLineWidth(0.6);
      doc.line(M, y, W - M, y);
      y += 18;
    } else {
      // Doesn't fit — start on a fresh page
      doc.addPage();
      y = M;
    }

    drawDetailsTable(
      y, sortedRows,
      'ROUTE DETAILS',
      subtitle,
      { showStopNumber: true }
    );

    return doc;

    // ----- HELPERS -----

    function drawSummaryCard(x, y, w, card) {
      const HEADER_H = 38;
      const PAD = 14;
      const data = card.data;
      const isEmpty = !data.length;

      // Layouts other than nationalByArt use a fixed row height. nationalByArt rows are
      // variable — each row's height depends on how many wrapped lines its allocated-art
      // file string occupies. We pre-compute per-row heights here so totalH can be set
      // accurately before the card background rectangle is drawn.
      const FIXED_ROW_H = card.layout === 'count' ? 18 : 18;
      // Per-row precomputed heights (used by nationalByArt layout). Key constants:
      //   BASE_H — 26pt of vertical space for the pill + primary line (+ optional
      //            secondary line if present). Smaller than the old 34pt because we
      //            now grow the row only as much as the actual art file content needs.
      //   ART_LINE_H — 9pt per wrapped art-file line at 7pt font.
      //   ART_TOP_GAP — 4pt breathing room between secondary text and art file lines.
      const BASE_H = 26;
      const ART_LINE_H = 9;
      const ART_TOP_GAP = 4;
      const ART_FONT_SIZE = 7;
      const labelMaxWPre = w - PAD * 2 - 6;

      const rowHeights = [];
      if (card.layout === 'nationalByArt') {
        for (const item of data) {
          const artFilesArr = item.artFiles ? [...item.artFiles] : [];
          let artLines = 0;
          if (artFilesArr.length) {
            doc.setFont('helvetica', 'normal');
            doc.setFontSize(ART_FONT_SIZE);
            const wrapped = doc.splitTextToSize(artFilesArr.join(' · '), labelMaxWPre - 4);
            artLines = wrapped.length;
          }
          const h = BASE_H + (artLines ? ART_TOP_GAP + artLines * ART_LINE_H : 0);
          rowHeights.push(h);
        }
      }

      const rowsToDraw = isEmpty ? 1 : data.length;
      const bodyH = card.layout === 'nationalByArt'
        ? rowHeights.reduce((s, h) => s + h, 0) + 12
        : rowsToDraw * FIXED_ROW_H + 12;
      const totalH = HEADER_H + bodyH;

      // Card background
      doc.setFillColor(255, 255, 255);
      doc.setDrawColor(232, 232, 230);
      doc.setLineWidth(0.6);
      doc.roundedRect(x, y, w, totalH, 8, 8, 'FD');

      // Accent bar (left edge)
      doc.setFillColor(card.accent[0], card.accent[1], card.accent[2]);
      doc.rect(x, y, 3, totalH, 'F');

      // Title
      doc.setFont('helvetica', 'bold');
      doc.setFontSize(10.5);
      doc.setTextColor(12, 10, 9);
      doc.text(card.title, x + PAD, y + 16);

      // Subtitle (sort hint, e.g. "sorted by SIO")
      if (card.subtitle) {
        doc.setFont('helvetica', 'normal');
        doc.setFontSize(7.5);
        doc.setTextColor(140, 140, 140);
        doc.text(card.subtitle, x + PAD, y + 28);
      }

      // Count badge (top-right)
      const total = isEmpty
        ? 0
        : data.reduce((s, d) => s + (typeof d.count === 'number' ? d.count : 1), 0);
      const badgeText = String(total);
      doc.setFont('helvetica', 'bold');
      doc.setFontSize(8);
      const bw = Math.max(20, doc.getTextWidth(badgeText) + 12);
      const bx = x + w - PAD - bw;
      const by = y + 8;
      doc.setFillColor(card.accent[0], card.accent[1], card.accent[2]);
      doc.roundedRect(bx, by, bw, 14, 7, 7, 'F');
      doc.setTextColor(255, 255, 255);
      doc.text(badgeText, bx + bw / 2, by + 9.5, { align: 'center' });

      // Header divider
      doc.setDrawColor(240, 240, 240);
      doc.setLineWidth(0.4);
      doc.line(x + PAD, y + HEADER_H - 4, x + w - PAD, y + HEADER_H - 4);

      // Body
      let cy = y + HEADER_H + 2;

      if (isEmpty) {
        doc.setFont('helvetica', 'normal');
        doc.setFontSize(9);
        doc.setTextColor(180, 180, 180);
        doc.text(card.empty, x + PAD, cy + 11);
        return y + totalH;
      }

      const labelMaxW = w - PAD * 2 - 6;

      for (const item of data) {
        if (card.layout === 'count') {
          // [Venue Name .................... count]
          doc.setFont('helvetica', 'normal');
          doc.setFontSize(9.5);
          doc.setTextColor(40, 40, 40);
          doc.text(fitText(doc, item.label, labelMaxW - 26), x + PAD, cy + 11);
          doc.setFont('helvetica', 'bold');
          doc.setTextColor(card.accent[0], card.accent[1], card.accent[2]);
          doc.text(String(item.count), x + w - PAD, cy + 11, { align: 'right' });
        } else if (card.layout === 'nationalByArt') {
          // Layout for one National Summary row:
          //   [S/B pill] [primary bold text] · [Kiosk Type] ............... [count]
          //                  [secondary grey text]
          //
          // Visibility / styling rules:
          //
          // 1. Art pill — a small square letter badge at the row's left edge:
          //      • S in grey for Standard art (the default — kept visually quiet)
          //      • B in purple for Boosted art (pops visually so warehouse spots them)
          //    For any non-Standard / non-Boosted art type (Digital With Bottom, etc.),
          //    we fall back to a small abbreviation pill in the card's accent color.
          //
          // 2. Kiosk Type — appended to the primary line as `· Premium`, `· Luxury`, etc.
          //    so the warehouse sees what kiosk hardware the artwork goes onto without
          //    needing to cross-reference another document.
          //
          // 3. Client vs Campaign promotion — many national tasks have an empty
          //    clientCompany. Rather than showing a bare em-dash on the bold line, we
          //    promote the campaign label to the primary line. If both are populated,
          //    client stays bold on top and campaign sits below in light grey.
          const artText = String(item.art || '—').toUpperCase();
          // Per dispatcher spec: the pill is ALWAYS S or B in the National
          // Summary card. Real-world OnFleet boost values come in many
          // shapes — "Standard", "Premium_Standard", "Boosted",
          // "Premium_Boosted", "BOOSTED AD", etc. — so we match leniently
          // on substring rather than requiring an exact 'STANDARD' /
          // 'BOOSTED' string. Default to Standard when ambiguous.
          const isBoosted  = /BOOST/.test(artText);
          const isStandard = !isBoosted;
          const clientText = (item.client || '').trim();
          // 'N/A' is the ingest fallback for missing customField values; treat it (and
          // 'NULL', em-dash, etc.) as no-client so we don't render a meaningless placeholder.
          const hasRealClient = clientText && !/^(n\/a|null|—|-|none)$/i.test(clientText);
          const primaryText = hasRealClient ? clientText : (item.campaign || '—');
          const secondaryText = hasRealClient ? (item.campaign || '') : '';

          // Small letter pill — 12pt square, sits flush at the left edge of the row
          const PILL_W = 12;
          const PILL_H = 11;
          let pillFill = [156, 163, 175];   // grey for Standard
          let pillLetter = 'S';
          if (isBoosted) {
            pillFill = [107, 43, 201];      // brand purple for Boosted
            pillLetter = 'B';
          }
          doc.setFillColor(pillFill[0], pillFill[1], pillFill[2]);
          doc.roundedRect(x + PAD, cy + 1, PILL_W, PILL_H, 2, 2, 'F');
          doc.setFont('helvetica', 'bold');
          doc.setFontSize(7.5);
          doc.setTextColor(255, 255, 255);
          doc.text(pillLetter, x + PAD + PILL_W / 2, cy + 8.5, { align: 'center' });

          const primaryStartX = x + PAD + PILL_W + 6;

          // Primary line — bold, dark. Append kiosk type as a quiet suffix when present.
          doc.setFont('helvetica', 'bold');
          doc.setFontSize(9);
          doc.setTextColor(40, 40, 40);
          const primaryMaxW = labelMaxW - (primaryStartX - (x + PAD)) - 22;
          const primaryFitted = fitText(doc, primaryText, primaryMaxW);
          doc.text(primaryFitted, primaryStartX, cy + 9);
          // Render the kioskType label after the primary text in lighter weight/color
          if (item.kioskType) {
            const consumedW = doc.getTextWidth(primaryFitted);
            const kTypeStartX = primaryStartX + consumedW + 6;
            const kTypeMaxW = primaryMaxW - consumedW - 8;
            if (kTypeMaxW > 20) {
              doc.setFont('helvetica', 'normal');
              doc.setFontSize(8.5);
              doc.setTextColor(120, 120, 120);
              doc.text('· ' + fitText(doc, item.kioskType, kTypeMaxW), kTypeStartX, cy + 9);
            }
          }

          // Count (right edge)
          doc.setFont('helvetica', 'bold');
          doc.setFontSize(10);
          doc.setTextColor(card.accent[0], card.accent[1], card.accent[2]);
          doc.text(String(item.count), x + w - PAD, cy + 10, { align: 'right' });

          // Per dispatcher spec: National Summary shows the BOLD campaign on
          // top + the dimmed Allocated Art file directly underneath. The
          // intermediate "secondary" line (a dimmed repeat of the campaign)
          // was dropped — it duplicated the bold primary text most of the
          // time since clientCompany and nationalLabel resolve to the same
          // string in our data.
          const artFilesArr = item.artFiles ? [...item.artFiles] : [];
          if (artFilesArr.length) {
            doc.setFont('helvetica', 'normal');
            doc.setFontSize(ART_FONT_SIZE);
            doc.setTextColor(140, 140, 140);
            const artFilesStr = artFilesArr.join(' · ');
            const wrapped = doc.splitTextToSize(artFilesStr, labelMaxW - 4);
            // Sits right under the bold primary line.
            const artY = cy + 21;
            wrapped.forEach((line, i) => {
              doc.text(line, x + PAD, artY + i * ART_LINE_H);
            });
          }
        }
        // Advance cy by this row's measured height (variable for nationalByArt, fixed for
        // other layouts). The rowHeights array is pre-computed at the top of this function
        // so totalH already accounts for these offsets.
        const idx = data.indexOf(item);
        cy += card.layout === 'nationalByArt' ? rowHeights[idx] : FIXED_ROW_H;
      }
      return y + totalH;
    }

    function drawDetailsTable(y, rows, title, subtitle, opts = {}) {
      // Landscape CONTENT_W ≈ 720pt — these proportions auto-scale to fill it.
      // Boosted rows are highlighted with a light purple row tint instead of an inline
      // pill or text column, which keeps the table visually clean while still letting
      // production spot Boosted pulls at a glance. Standard rows stay neutral.
      const allCols = [
        { key: 'stop',      label: '#',                 w: 28 },
        { key: 'kiosk',     label: 'Kiosk ID',          w: 50 },
        { key: 'kioskLoc',  label: 'Location in Venue', w: 95 },
        { key: 'type',      label: 'Task Type',         w: 70 },
        { key: 'venue',     label: 'Venue',             w: 80 },
        { key: 'clientCol', label: 'Client / Campaign', w: 205 },
        { key: 'address',   label: 'Venue Location',    w: 180 },
        { key: 'state',     label: 'St',                w: 32 },
      ];
      // Locals don't need the venue's street address — they're scoped to a single venue
      // and the warehouse uses this section to pull artwork by SIO, not for routing.
      // Locals also don't need stop # since they're sorted by SIO, not by route order.
      // For Locals, "Location in Venue" is replaced by "SIO" — that's the artwork pull key
      // that matters there. Route Details keeps "Location in Venue".
      let cols = allCols;
      if (opts.hideAddress)     cols = cols.filter(c => c.key !== 'address');
      if (!opts.showStopNumber) cols = cols.filter(c => c.key !== 'stop');
      if (opts.useSioColumn) {
        // Replace "Location in Venue" with "SIO"
        cols = cols.map(c => c.key === 'kioskLoc'
          ? { key: 'sio', label: 'SIO', w: c.w }
          : c);
      }
      if (opts.showArtFile) {
        // Insert "Art File" column directly after Client / Campaign so the warehouse can
        // read the campaign and the specific artwork file together.
        const clientIdx = cols.findIndex(c => c.key === 'clientCol');
        if (clientIdx !== -1) {
          // Trim Client/Campaign a touch to make room
          cols[clientIdx] = { ...cols[clientIdx], w: 130 };
          cols.splice(clientIdx + 1, 0, { key: 'artFile', label: 'Art File', w: 145 });
        }
      }
      // Locals-only layout: a fully custom column order designed for warehouse pulling.
      // Order: Notes · Kiosk ID · SIO · Kiosk Type · Task Type · Venue ·
      //        Client/Campaign · Allocated Art · State.
      // Boosted rows are highlighted with a light purple background tint at the row
      // level — see the row-fill block below — so we don't need a dedicated Art column.
      if (opts.localsLayout) {
        const byKey = Object.fromEntries(cols.map(c => [c.key, c]));
        const built = [
          { key: 'notes',     label: 'Notes',            w: 85 },
          byKey.kiosk     && { ...byKey.kiosk,    w: 50 },
          byKey.sio       && { ...byKey.sio,      w: 70 },
          { key: 'kioskType', label: 'Kiosk Type',       w: 55 },
          byKey.type      && { ...byKey.type,     w: 60 },
          { key: 'venue',     label: 'Venue',            w: 70 },
          byKey.clientCol && { ...byKey.clientCol, w: 168 },
          byKey.artFile   && { ...byKey.artFile,  label: 'Allocated Art', w: 110 },
          byKey.state     && { ...byKey.state,    w: 32 },
        ].filter(Boolean);
        cols = built;
      }
      const totalW = cols.reduce((s,c) => s + c.w, 0);
      const scale = CONTENT_W / totalW;
      cols.forEach(c => c.w *= scale);

      const HEAD_H = 22;
      // ROW_H grows when this table renders an Allocated Art sub-line beneath
      // the campaign in the Client/Campaign cell. Tables that don't have a
      // clientCol column (or where no row has artFile) keep the compact 22pt.
      // The taller 30pt buys ~9pt for the dim sub-line below the campaign.
      const _hasClientCol = cols.some(c => c.key === 'clientCol');
      const _anyArtFileInClientCol = _hasClientCol && rows.some(r => (r && r.artFile && String(r.artFile).trim()));
      const ROW_H = _anyArtFileInClientCol ? 30 : 22;
      // Title (12pt) + column header (22pt) + at least one data row (22pt) = 56pt minimum.
      // If we don't have that much room, push the entire table to the next page rather than
      // stranding an orphan title+header on the bottom of the previous page.
      const minNeeded = 12 + HEAD_H + ROW_H + 6;
      if (y + minNeeded > H - M) {
        doc.addPage();
        y = M;
      }

      // Section header
      const sectionTitle = (title || 'DETAIL').toUpperCase();
      const sectionSubtitle = subtitle || `${rows.length} task${rows.length === 1 ? '' : 's'}`;

      // Title (with letter-spacing)
      doc.setFont('helvetica', 'bold');
      doc.setFontSize(8);
      doc.setTextColor(107, 43, 201);
      doc.setCharSpace(1.2);
      doc.text(sectionTitle, M, y);
      // Measure title width WITH the charSpace applied (bigger than raw getTextWidth)
      const titleW = doc.getTextWidth(sectionTitle) + 1.2 * sectionTitle.length + 14;
      doc.setCharSpace(0);
      // Subtitle in muted grey, sits to the right of the title
      doc.setFont('helvetica', 'normal');
      doc.setTextColor(140, 140, 140);
      doc.text(sectionSubtitle, M + titleW, y);
      y += 12;

      // Header row
      doc.setFillColor(248, 248, 247);
      doc.rect(M, y, CONTENT_W, HEAD_H, 'F');
      doc.setDrawColor(220, 220, 218);
      doc.setLineWidth(0.6);
      doc.line(M, y, M + CONTENT_W, y);
      doc.line(M, y + HEAD_H, M + CONTENT_W, y + HEAD_H);

      doc.setFont('helvetica', 'bold');
      doc.setFontSize(7.5);
      doc.setTextColor(107, 43, 201);
      doc.setCharSpace(0.4);

      let cx = M;
      for (const c of cols) {
        doc.text(fitText(doc, c.label.toUpperCase(), c.w - 8), cx + 5, y + 14);
        cx += c.w;
      }
      doc.setCharSpace(0);

      let cy = y + HEAD_H;

      doc.setFont('helvetica', 'normal');
      doc.setFontSize(8.5);

      // ============ GROUPING (locals only) ============
      // When opts.groupLocals is set, collapse rows that share their artwork-pull fields
      // (SIO, Campaign, Task Type, Kiosk Type, Notes, Allocated Art, State) into a single
      // master row + an indented sub-line listing each kiosk. Venue is allowed to vary
      // within a group — it shows up per-kiosk in the sub-line. This compresses common
      // patterns like "8 kiosks, all on the same campaign at varying Safeway/Albertsons
      // venues" from 8 visually-identical rows into 1 master + 1 sub-line.
      //
      // Each renderItem is one of:
      //   { kind: 'row',   row }                        — single-row render (standard)
      //   { kind: 'group', master, kiosks, allSameVenue } — master row + sub-line
      const renderItems = [];
      if (opts.groupLocals) {
        const groupKey = r => [
          r.sio || '', r.campaign || '', r.type || '', r.kioskType || '',
          (r.notes || '').replace(/\s+/g, ' ').trim(),
          r.artFile || '', r.stateCode || ''
        ].join('§');
        const groupMap = new Map();
        for (const r of rows) {
          const key = groupKey(r);
          if (groupMap.has(key)) {
            groupMap.get(key).push(r);
          } else {
            groupMap.set(key, [r]);
          }
        }
        // Iterate groups in the order their first row appears (preserves SIO sort order)
        const seen = new Set();
        for (const r of rows) {
          const key = groupKey(r);
          if (seen.has(key)) continue;
          seen.add(key);
          const groupRows = groupMap.get(key);
          if (groupRows.length === 1) {
            renderItems.push({ kind: 'row', row: groupRows[0] });
          } else {
            const venues = [...new Set(groupRows.map(g => (g.venue || '').trim()).filter(Boolean))];
            const allSameVenue = venues.length <= 1;
            // Master row: synthesize a row from the first group member, but show the
            // count in the Kiosk ID column and "(varies)" in the Venue column when
            // venues differ across the group.
            const master = {
              ...groupRows[0],
              kiosk: '',                        // rendered as a bubble in the cell — see kiosk cell case below
              venue: allSameVenue ? (venues[0] || groupRows[0].venue) : '(varies)',
              _isGroupMaster: true,
              _groupCount: groupRows.length,
            };
            renderItems.push({ kind: 'group', master, kiosks: groupRows, allSameVenue });
          }
        }
      } else {
        for (const r of rows) renderItems.push({ kind: 'row', row: r });
      }

      // Sub-block height (used only by groups). The grouped-locals sub-block now lists
      // each kiosk on its own line — like a dropdown — instead of a single comma-joined
      // line. SUB_ROW_H is the per-kiosk height; SUB_HEAD_H is the small column header
      // row drawn above the kiosks; SUB_PAD is breathing room above/below.
      // Per dispatcher spec: sub-row text should be exactly 0.5pt smaller than
      // the master row text (master = 8.5pt → sub = 8.0pt). SUB_ROW_H bumps to
      // 9 so the slightly larger text still has a touch of vertical breathing room.
      const SUB_ROW_H = 9;
      const SUB_HEAD_H = 9;
      const SUB_PAD = 3;
      const SUB_FONT = 8.0;

      let zebra = false;
      for (const item of renderItems) {
        const r = item.kind === 'group' ? item.master : item.row;
        const isMasterGroup = item.kind === 'group';
        // Group sub-block grows with the number of kiosks: column-header row + one row per kiosk + top/bottom padding
        const subBlockH = isMasterGroup
          ? SUB_HEAD_H + item.kiosks.length * SUB_ROW_H + SUB_PAD * 2
          : 0;
        const thisRowH = ROW_H + subBlockH;

        // Page break if needed (account for sub-block height when grouped)
        if (cy + thisRowH > H - M) {
          doc.addPage();
          cy = M;

          // Mini header on continuation pages
          doc.setFont('helvetica', 'bold');
          doc.setFontSize(8);
          doc.setTextColor(107, 43, 201);
          doc.setCharSpace(1.2);
          doc.text('PACKING SLIP · ' + worker.toUpperCase() + ' · CONT.', M, cy + 6);
          doc.setCharSpace(0);
          cy += 18;

          // Re-draw column header
          doc.setFillColor(248, 248, 247);
          doc.rect(M, cy, CONTENT_W, HEAD_H, 'F');
          doc.setDrawColor(220, 220, 218);
          doc.setLineWidth(0.6);
          doc.line(M, cy, M + CONTENT_W, cy);
          doc.line(M, cy + HEAD_H, M + CONTENT_W, cy + HEAD_H);
          doc.setFont('helvetica', 'bold');
          doc.setFontSize(7.5);
          doc.setTextColor(107, 43, 201);
          doc.setCharSpace(0.4);
          let hx = M;
          for (const c of cols) {
            doc.text(fitText(doc, c.label.toUpperCase(), c.w - 8), hx + 5, cy + 14);
            hx += c.w;
          }
          doc.setCharSpace(0);
          cy += HEAD_H;
          doc.setFont('helvetica', 'normal');
          doc.setFontSize(8.5);
        }

        const rowIsIncorrect = isIncorrect(r);
        // "Omitted" rows = continuity/photo tasks that are still listed in Route Details
        // for the worker's reference but get dimmed so production ignores them. They never
        // appear in Locals (so this flag is naturally false there).
        const rowIsOmitted = !isPackable(r);
        // Boosted rows get a light purple tint at the row level — production scanning the
        // slip can spot every Boosted pull at a glance without needing a dedicated column
        // or pill. Standard rows stay on the neutral white/zebra background.
        //
        // SUPPRESSION RULE: OnFleet sometimes tags removal/default tasks (Pull Down, Remove
        // Kiosk, Default, etc.) as Boosted in the boostedStandard field, which is a data
        // inconsistency — Boosted only makes sense for a real ad install. Treat any of
        // these task-type families as NOT boosted regardless of what the boost field says,
        // so the purple tint never decorates a removal row.
        const ttLower = String(r.type || '').toLowerCase();
        const sioLower = String(r.sio || '').toLowerCase();
        const isRemovalLike =
          isDefault(r)                            // already catches Pull Down + Default
          || /\bremoval?\b/.test(ttLower)         // "Remove Kiosk", "Kiosk Removal", etc.
          || /\bins\/remove\b/.test(ttLower)      // "Digital INS/Remove"
          || ttLower === 'default'                // bare-default task type
          || sioLower === 'default' || sioLower === '0';
        const rowIsBoosted =
          String(r.boost || '').toUpperCase() === 'BOOSTED'
          && !rowIsOmitted
          && !isRemovalLike;

        // Row background — red wash for incorrect takes priority, then Boosted purple
        // tint, then zebra. Omitted rows always stay neutral so dimming reads cleanly.
        if (rowIsIncorrect && !rowIsOmitted) {
          doc.setFillColor(254, 242, 242);   // soft red wash
          doc.rect(M, cy, CONTENT_W, ROW_H, 'F');
        } else if (rowIsBoosted) {
          // Light brand-purple wash — readable on warehouse printers without overwhelming
          // the row content. Picked to clearly distinguish from white and the very subtle
          // zebra grey while staying tasteful at print resolution.
          doc.setFillColor(232, 218, 250);
          doc.rect(M, cy, CONTENT_W, ROW_H, 'F');
        } else if (zebra) {
          doc.setFillColor(252, 252, 251);
          doc.rect(M, cy, CONTENT_W, ROW_H, 'F');
        }
        zebra = !zebra;

        // Row separator
        doc.setDrawColor(238, 238, 236);
        doc.setLineWidth(0.3);
        doc.line(M, cy + ROW_H, M + CONTENT_W, cy + ROW_H);

        // Left edge accent — suppressed entirely for omitted rows
        if (!rowIsOmitted) {
          if (rowIsIncorrect) {
            doc.setFillColor(220, 38, 38);     // red
            doc.rect(M, cy, 3, ROW_H, 'F');
          } else if (r.isInstall && !opts.deemphasizeInstalls) {
            doc.setFillColor(22, 163, 74);
            doc.rect(M, cy, 2, ROW_H, 'F');
          }
        }

        // Cells
        cx = M;
        for (const c of cols) {
          let v = '';
          let valFont = 'normal';
          let valColor = [40, 40, 40];
          // Optional sub-line rendered BELOW the main cell content. Currently
          // used by the Client/Campaign cell to surface the Allocated Art file
          // name in a slightly darker dim — readable but visually subordinate
          // to the campaign text above it. Empty string = no sub-line.
          let subText = '';
          let subColor = [110, 110, 110];

          switch (c.key) {
            case 'stop':
              v = r.stopNumber ? String(r.stopNumber) : '—';
              valFont = 'bold';
              valColor = r.stopNumber ? [107, 43, 201] : [180, 180, 180];
              break;
            case 'kiosk':
              // Group masters: render a small filled bubble showing the group's kiosk
              // count instead of a text value. The dropdown sub-block below the master
              // row enumerates the actual kiosk IDs, so the bubble's job is just to
              // signal "this row represents N stops" at a glance.
              if (r._isGroupMaster && r._groupCount) {
                const count = r._groupCount;
                const countText = String(count);
                doc.setFont('helvetica', 'bold');
                doc.setFontSize(8.5);
                const txtW = doc.getTextWidth(countText);
                // Bubble diameter scales for double-digit counts so the number always fits
                const BUB_H = 13;
                const BUB_W = Math.max(BUB_H, txtW + 10);
                const bx = cx + 3;
                const by = cy + (ROW_H - BUB_H) / 2;
                // Filled brand-purple bubble; if this row also got a Boosted/Incorrect
                // tint at the row level, the bubble still reads clearly because it's
                // filled & high contrast against any of those backgrounds.
                doc.setFillColor(107, 43, 201);
                doc.roundedRect(bx, by, BUB_W, BUB_H, BUB_H / 2, BUB_H / 2, 'F');
                doc.setTextColor(255, 255, 255);
                doc.text(countText, bx + BUB_W / 2, by + 9, { align: 'center' });
                cx += c.w;
                continue;  // skip the text-rendering tail at the end of the loop
              }
              v = r.kiosk || '';
              // Render in normal weight per dispatcher spec — bold was making
              // the column stand out too aggressively and competing with the
              // bold #/SIO columns for attention.
              break;
            case 'venue':
              v = r.venue || '';
              break;
            case 'kioskLoc':
              v = r.kioskLoc || '';
              valColor = [21, 128, 61];
              break;
            case 'type': {
              // Per warehouse spec, the displayed task type follows two override rules:
              //   - Pull Down → display as "Default" (artwork removal — already counted
              //     in the Defaults bucket via isDefault upstream).
              //   - Digital Default (default-classified task on a Premium Digital kiosk
              //     without "Magnet" in the task type) → display as "Remove Magnet".
              //     These tasks have no paper to pull but the field worker still has to
              //     clear the existing magnet from the kiosk's paper slot. The row is
              //     also dimmed via rowIsOmitted because Digital Defaults are flagged
              //     non-packable.
              const tt = r.type || '';
              const ttLower = tt.toLowerCase();
              const sioLower = String(r.sio || '').toLowerCase();
              const boostLower = String(r.boost || '').toLowerCase();
              const cnLower = String(r.campaign || '').toLowerCase();
              const ktLower = String(r.kioskType || '').toLowerCase();
              const isDefBool =
                sioLower === 'default' || sioLower === '0'
                || boostLower === 'default ad' || boostLower === 'default'
                || ttLower === 'default'
                || /\bpull\s*down\b/.test(ttLower) || /\bpull\s*down\b/.test(cnLower)
                || /\breplace\s+with\s+default\b/.test(cnLower);
              const isDigDef = isDefBool && ktLower === 'digital' && !/magnet/.test(ttLower);
              if (isDigDef) {
                v = 'Remove Magnet';
              } else if (/\bpull\s*down\b/i.test(tt)) {
                v = 'Default';
              } else {
                v = tt;
              }
              if (r.isInstall && !opts.deemphasizeInstalls) {
                valFont = 'bold';
                valColor = [22, 163, 74];
              }
              break;
            }
            case 'boost':
              v = r.boost || '';
              break;
            case 'sio': {
              // SIO field has a few "no-value" sentinels OnFleet uses: literal "Default"/
              // "DEFAULT", "NULL", "0", and bare empty. They all mean the same thing —
              // there's no specific SIO order, this row uses default artwork. Normalize
              // them to the dim "Default" rendering so the warehouse reads them consistently.
              const raw = (r.sio || '').toString().trim();
              const isDefault = !raw || /^(default|null|0)$/i.test(raw);
              if (isDefault) {
                v = 'Default';
                valColor = [180, 180, 180];
              } else {
                v = raw;
                valFont = 'bold';
              }
              break;
            }
            case 'clientCol': {
              // Plain text: "Client - Campaign - Customer Type", dim style matching other cells.
              // ALWAYS use campaignName for the campaign portion (nationalCampName is unreliable —
              // it's sometimes the wrong field, sometimes a literal placeholder like "Local").
              const clean = p => {
                if (p == null) return '';
                const s = String(p).trim();
                if (/^(n\/a|null|—)$/i.test(s)) return '';
                return s;
              };
              const cust = clean(r.customerType);
              const custLower = cust.toLowerCase();
              const custNice = cust ? cust.charAt(0).toUpperCase() + cust.slice(1).toLowerCase() : '';

              let campaignField = clean(r.campaign);
              // Drop a campaign value that's literally the same as the customer type
              // (e.g. campaignName="Local" with customerType="Local" → redundant).
              if (campaignField && campaignField.toLowerCase() === custLower) {
                campaignField = '';
              }

              // De-duplicate redundant pieces. OnFleet's free-form fields often have
              // the client name baked into the campaign string ("Cindy Gee - Keller
              // Williams" with client "Cindy Gee"), or the campaign repeated twice in
              // a row, etc. The cell renderer ends up concatenating client + campaign
              // and the result reads like "Cindy Gee - Cindy Gee - Keller Williams"
              // unless we strip the duplication. Both the client and campaign fields
              // can be multi-segment ("Cindy Gee - Keller Williams"), so we split BOTH
              // on " - " and dedupe at the segment level. Comparison is case-insensitive
              // and ignores punctuation/whitespace.
              const norm = s => String(s || '').toLowerCase().replace(/[^\w]+/g, '');
              const clientField = clean(r.clientCompany);
              if (campaignField) {
                const seen = new Set();
                // Seed the seen set with each segment of the client field so subsequent
                // matches in the campaign get suppressed
                if (clientField) {
                  for (const seg of clientField.split(/\s+-\s+/)) {
                    const k = norm(seg);
                    if (k) seen.add(k);
                  }
                }
                const segs = campaignField.split(/\s+-\s+/);
                const kept = [];
                for (const seg of segs) {
                  const k = norm(seg);
                  if (!k) continue;
                  if (seen.has(k)) continue;
                  seen.add(k);
                  kept.push(seg.trim());
                }
                campaignField = kept.join(' - ');
              }

              const parts = [
                clientField,
                campaignField,
                custNice,
              ].filter(Boolean);
              v = parts.length ? parts.join(' - ') : '—';
              if (!parts.length) valColor = [180, 180, 180];
              // Allocated Art surfaces under the campaign as a dimmed sub-line.
              // Slightly darker (110,110,110) than the standard dim (140,140,140)
              // so warehouse can still scan the filename quickly without it
              // competing with the main campaign text above. Drawn AFTER the
              // main cell text via the post-loop render block below.
              const _af = (r.artFile || '').trim();
              if (_af) subText = _af;
              break;
            }
            case 'artFile': {
              const af = (r.artFile || '').trim();
              v = af || '—';
              if (!af) valColor = [180, 180, 180];
              break;
            }
            case 'kioskType': {
              v = (r.kioskType || '').trim();
              if (!v) { v = '—'; valColor = [180, 180, 180]; }
              break;
            }
            case 'notes': {
              // Raw OnFleet task notes, condensed for tabular display. The "art file"
              // extraction we ran during ingest already pulled out structured artwork
              // references; this column shows whatever else lives in notes — driver
              // instructions, special handling, contact info, etc.
              const raw = (r.notes || '').trim();
              if (raw) {
                // Collapse whitespace/newlines to a single space so the cell stays compact.
                v = raw.replace(/\s+/g, ' ');
              } else {
                v = '—';
                valColor = [180, 180, 180];
              }
              break;
            }
            case 'state':
              v = r.stateCode || '';
              valFont = 'bold';
              break;
            case 'address': {
              const a = r.address || '';
              v = (!a || /^n\/a$/i.test(a)) ? '—' : a;
              if (v === '—') valColor = [180, 180, 180];
              break;
            }
          }

          // Omitted rows (continuity/photo) are visually muted so production knows to skip
          // them. Override any per-cell coloring/weight from the switch with a uniform grey.
          if (rowIsOmitted) {
            valColor = [185, 185, 185];
            valFont = 'normal';
          }

          doc.setFont('helvetica', valFont);
          doc.setTextColor(valColor[0], valColor[1], valColor[2]);
          // The state column is always a 2-letter abbreviation (CA, NY, MA, ...) — never
          // wrap it across two lines. Other columns wrap up to 2 lines when content is long.
          let _mainLineCount = 1;
          if (c.key === 'state') {
            doc.text(String(v), cx + 5, cy + 13);
          } else {
            const lines = doc.splitTextToSize(String(v), c.w - 8);
            // When a subText (Allocated Art) is in play and ROW_H expanded to
            // 30pt, force the main content to a single line so the sub-line
            // has a clean place to sit underneath. Without subText we keep the
            // prior 2-line wrapping budget.
            const maxMainLines = (subText && ROW_H >= 30) ? 1 : 2;
            const displayed = lines.slice(0, maxMainLines);
            _mainLineCount = displayed.length;
            doc.text(displayed, cx + 5, cy + (_mainLineCount > 1 ? 9 : 13));
          }
          // Optional dimmed sub-line — used by clientCol to render the
          // Allocated Art filename below the campaign text. Slightly darker
          // than standard dim (110 vs 140) so it stays scannable on a printed
          // slip without overpowering the primary line above it.
          if (subText) {
            doc.setFont('helvetica', 'normal');
            doc.setFontSize(7);
            doc.setTextColor(subColor[0], subColor[1], subColor[2]);
            const subLines = doc.splitTextToSize(String(subText), c.w - 8);
            // Place the sub-line at the bottom of the row, leaving a tiny
            // breath of whitespace between it and the bottom border.
            doc.text(subLines.slice(0, 1), cx + 5, cy + ROW_H - 4);
            // Restore the row loop's default 8.5pt font for subsequent cells.
            doc.setFontSize(8.5);
          }
          cx += c.w;
        }

        // Sub-block for grouped masters — each kiosk rendered on its own row in a small
        // 4-column table (Kiosk ID · Task Type · Venue Name · Allocated Art). Drawn
        // directly under the master row in a quiet grey panel with column headers so the
        // sub-block reads as its own mini-table while still visually attached to the
        // master row above. In practice all four columns hold the same value across rows
        // in a given group (that's WHY they're grouped), but the headers + per-row
        // repetition make the slip explicit and unambiguous for production.
        if (isMasterGroup) {
          const subY = cy + ROW_H;
          // Subtle separator background extending the master row's color into the
          // sub-block so the master + kiosks read as a single visual unit
          doc.setFillColor(252, 250, 248);
          doc.rect(M, subY, CONTENT_W, subBlockH, 'F');
          // A faint left rule running the full sub-block height anchors the kiosks to
          // the master row above. Keeps the dropdown feeling — like an indent guide.
          doc.setFillColor(220, 215, 230);
          doc.rect(M + 2, subY + 2, 1.4, subBlockH - 4, 'F');

          // Sub-table column geometry. The sub-table begins where the Client/Campaign
          // column begins on the master row — that's the wide "narrative" column where
          // the campaign info lives, so visually the dropdown reads as the breakdown of
          // that column. Sum the widths of every column LEFT of clientCol to compute
          // the indent. The Allocated Art column on the right then receives whatever
          // horizontal space remains.
          let indentW = 0;
          for (const c of cols) {
            if (c.key === 'clientCol') break;
            indentW += c.w;
          }
          const subStartX = M + indentW + 4;
          const subAvailW = CONTENT_W - indentW - 8;

          // Sub-table columns are decided per-group based on which fields actually vary
          // across the group's kiosks. Fields that are uniform across every row are
          // omitted from the sub-table since they're already shown on the master row —
          // repeating them just consumes horizontal space production has to skip past.
          //
          // Always shown: # · Kiosk ID · Kiosk Type · Allocated Art
          //   (Kiosk Type is per-kiosk because production needs to grab the correct
          //   hardware variant — Premium Mini vs Sani vs Digital — even when the
          //   campaign and venue match. Always-on per spec.)
          // Conditionally shown:
          //   - Task Type — included only when kiosks in the group have different
          //     task types (e.g. one Escalation + one New Ad). Uniform groups omit it.
          //   - Venue Name — included only when venues vary; the master row's "(varies)"
          //     marker tells the reader to look at the sub-table for the breakdown.
          const distinctTypes = new Set(item.kiosks.map(k => (k.type || '').trim()));
          const showType = distinctTypes.size > 1;
          const distinctVenues = new Set(item.kiosks.map(k => (k.venue || '').trim()));
          const showVenue = distinctVenues.size > 1;

          // Width allocation — proportional within whatever columns are present so the
          // remaining art-file column always absorbs spare width.
          const baseSlots = { idx: 18, kiosk: 0.16, kioskType: 0.13 };
          if (showType)  baseSlots.type = 0.16;
          if (showVenue) baseSlots.venue = 0.20;
          const subAvailMinusIdx = subAvailW - baseSlots.idx;
          const propTotal = Object.values(baseSlots).filter(v => v < 1).reduce((s, v) => s + v, 0);
          // Reserve ~0.30 of available width for the artFile column at the end
          const subColW = { idx: baseSlots.idx };
          for (const k of Object.keys(baseSlots)) {
            if (k === 'idx') continue;
            subColW[k] = Math.round(subAvailMinusIdx * (baseSlots[k] / (propTotal + 0.30)));
          }
          // Allocated Art absorbs whatever's left
          const usedW = Object.values(subColW).reduce((s, v) => s + v, 0);
          subColW.artFile = subAvailW - usedW;

          const subCols = [
            { key: 'idx',       label: '#',             w: subColW.idx },
            { key: 'kiosk',     label: 'KIOSK ID',      w: subColW.kiosk },
            { key: 'kioskType', label: 'KIOSK TYPE',    w: subColW.kioskType },
            ...(showType  ? [{ key: 'type',  label: 'TASK TYPE',  w: subColW.type  }] : []),
            ...(showVenue ? [{ key: 'venue', label: 'VENUE NAME', w: subColW.venue }] : []),
            { key: 'artFile',   label: 'ALLOCATED ART', w: subColW.artFile },
          ];

          // Header row — small caps, dim, with light underline so it reads as a header
          const headY = subY + SUB_PAD + 7;
          doc.setFont('helvetica', 'bold');
          doc.setFontSize(6);
          doc.setTextColor(150, 150, 150);
          doc.setCharSpace(0.5);
          let hx = subStartX;
          for (const c of subCols) {
            doc.text(c.label, hx, headY);
            hx += c.w;
          }
          doc.setCharSpace(0);
          // Faint underline beneath the header row
          doc.setDrawColor(225, 222, 220);
          doc.setLineWidth(0.3);
          doc.line(subStartX, subY + SUB_PAD + SUB_HEAD_H - 1, subStartX + subAvailW, subY + SUB_PAD + SUB_HEAD_H - 1);

          // Data rows
          doc.setFont('helvetica', 'normal');
          doc.setFontSize(SUB_FONT);
          item.kiosks.forEach((k, idx) => {
            const rowY = subY + SUB_PAD + SUB_HEAD_H + idx * SUB_ROW_H + 5.5;
            let cellX = subStartX;
            for (const c of subCols) {
              const raw =
                c.key === 'idx'       ? `${idx + 1}.` :
                c.key === 'kiosk'     ? (k.kiosk || '—') :
                c.key === 'kioskType' ? ((k.kioskType || '').trim() || 'Premium') :
                c.key === 'type'      ? (k.type  || '—') :
                c.key === 'venue'     ? (k.venue || '—') :
                c.key === 'artFile'   ? ((k.artFile || '').trim() || '—') :
                '';
              // Kiosk ID is the eye anchor of each row — the rest read as supporting context
              const isAnchor = c.key === 'kiosk';
              doc.setTextColor(isAnchor ? 80 : 130, isAnchor ? 80 : 130, isAnchor ? 80 : 130);
              // Truncate to fit the cell width with a small inner padding
              const fitted = fitText(doc, String(raw), c.w - 6);
              doc.text(fitted, cellX, rowY);
              cellX += c.w;
            }
            // Dim separator line between rows — drawn below every row except the last
            // so the rows visually divide cleanly without a trailing empty divider.
            // Color is intentionally faint (lighter than the column-header underline)
            // so it reads as a subtle row demarcation rather than a heavy grid.
            if (idx < item.kiosks.length - 1) {
              const sepY = subY + SUB_PAD + SUB_HEAD_H + (idx + 1) * SUB_ROW_H - 1;
              doc.setDrawColor(232, 230, 226);
              doc.setLineWidth(0.25);
              doc.line(subStartX, sepY, subStartX + subAvailW, sepY);
            }
          });
        }

        cy += thisRowH;
      }
      return cy;
    }
  }


"""

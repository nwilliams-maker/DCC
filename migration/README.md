# DCC database migration

Moves DCC off the Google Sheet (`IC_SHEET_URL`) + Apps Script (`GAS_WEB_APP_URL`)
backend onto a real Postgres database. Full context, the current
architecture, and the open questions are in the migration plan doc; this
folder is the code half of that plan.

Decision on file: **hard cutover, no parallel-run window** (no dual-write
phase) — so the staging import in step 2 and the verification pass in step 4
are the only safety net before flipping the switch. Budget real time for
both.

## Files

- `schema.sql` — the six tables that replace the seven Sheet tabs.
- `import_from_sheets.py` — one-time import from the live Sheet into the new database. Safe to re-run (every insert is an upsert). Does **not** import the Archive tab's history — `route_events` starts empty and fills in going forward as the app runs (decision on file: don't carry over old archive history).
- `data_access.py` — the new data-access layer: one function per Sheet-read / GAS-write the app does today. Meant to be dropped into `tactical_workspace_master_rw.py` in place of the CSV-fetch and `requests.post(GAS_WEB_APP_URL, ...)` call sites.
- `requirements.txt` — `sqlalchemy` + `psycopg2-binary`, on top of the app's existing `requirements.txt`.

## Steps

1. **Provision Postgres.** Add a Postgres instance in the same Railway project as the app (private networking to it is automatic). Set `DATABASE_URL` in both your local shell (for the steps below) and the app's Railway environment variables.
2. **Create the schema.**
   ```
   psql "$DATABASE_URL" -f migration/schema.sql
   ```
3. **Import from the Sheet, against staging first.** Point `DATABASE_URL` at a throwaway/staging database, then:
   ```
   pip install -r requirements.txt -r migration/requirements.txt
   export IC_SHEET_URL="<the Sheet's edit URL>"
   python migration/import_from_sheets.py
   ```
   Compare row counts (`SELECT COUNT(*) FROM contractors`, etc.) against the live Sheet, and spot-check a handful of records by hand, especially route `payload` JSON. Re-run against the real `DATABASE_URL` once you're satisfied.
4. **Wire `data_access.py` into the app.** Swap each read/write call site in `tactical_workspace_master_rw.py` for the matching function here — see the migration doc's "Code changes required" section for the full action-to-function mapping. Keep the existing `@st.cache_data` decorators wrapping the new read functions so caching behavior doesn't change. **Read this alongside "Step 4/5, in practice" below first** — it's a bigger job than a mechanical find-and-replace, and three of the write actions have hidden OnFleet/Monday.com side effects that need a decision before they're safe to swap. Do this against a running staging instance, not statically.
5. **Build the portal endpoint.** `docs/portal-dcc-rw.html` posts `processDecision` straight to GAS today and can't hold a database credential (it's public static HTML). It needs a small API endpoint in front of `data_access.process_decision()` — this is the one new piece of infrastructure in this migration, not a straight port. **Also see "Step 4/5, in practice"**: the live `processDecision` triggers an OnFleet auto-assign that `data_access.process_decision()` doesn't replicate — don't build this endpoint as a bare wrapper around it until that's resolved.
6. **Repoint the portal and the Field Nation browser extension** at the new endpoint(s), and confirm with whoever owns the extension that its three bulk-provider actions are updated too.
7. **Decide on the OnFleet/Monday.com orchestration** hidden in three GAS actions today: `markFNAssigned` (route-plan rename, worker updates, Monday.com mutations), `processDecision` (OnFleet auto-assign on accept), and `saveToFieldNation` (a Monday.com placeholder push). None of these are replicated by their `data_access.py` counterparts, which only touch the database row. Port them into Python or keep them as a small separate service called from `data_access.py` — see `data_access.py`'s module docstring and "Step 4/5, in practice" below.
8. **Cut over.** Since this is a hard cutover: do the final verification pass against staging (row counts, spot-checked records, a live test session pointed at the new database) as the last gate, then switch the app's env vars and redeploy. Retire `GAS_WEB_APP_URL`, `IC_SHEET_URL`, and `DCC_SHARED_SECRET` once everything is confirmed live on the new backend.

## Dry-run verification (2026-09-18, local sandbox — not staging)

Ran `schema.sql` + the real, unmodified `import_from_sheets.py` end-to-end
against a throwaway local Postgres, feeding it the live Sheet's actual data
(already fetched once this session for an unrelated data-quality check) via
a one-line patch of `fetch_csv` instead of a network call — every other line
of the import script ran as-is. This is **not** a substitute for step 3
against real staging — it never touched `DATABASE_URL`/`IC_SHEET_URL` or any
live infrastructure — but it does confirm the schema and script are correct
before anyone spends time on a real staging run:

- `schema.sql` applies cleanly, no errors.
- Import completed with no exceptions: 564 contractor rows → 562 distinct
  (the 2 known duplicate emails correctly collapsed via upsert), 193 route
  rows across sent/accepted/declined/finalized → 190 distinct routes (the
  known in-tab duplicates and the Saved→Accepted lifecycle transition both
  resolved exactly as expected, no data loss), 74 Field Nation orders (no
  dupes, matches the earlier audit), `route_events` stayed at 0 (confirms
  the Archive skip).
- Robert Nieman's row confirmed in the database with `phone = '18186324368'` —
  the override works in practice, not just in theory.
- Spot-checked: `unrestricted = true` on exactly the 4 contractors matching
  the name-fragment list; `routes.payload`/`locs`/`stop_data` are valid
  JSON/JSONB on every sampled row; no NULL `wo` or blank `contractor_name`
  in `routes`.

Bottom line: the import side of this migration (schema + `import_from_sheets.py`,
including both data fixes above) is verified correct. What's still unverified
is everything real infrastructure touches — provisioning, the actual staging
`DATABASE_URL`, and the app-wiring work below.

## Step 4/5, in practice: why they're not a mechanical swap

Attempted step 4 (wiring `data_access.py` into the app) and step 5 (the
portal endpoint) directly, without a staging `DATABASE_URL` available to test
against. Stopped short of touching `tactical_workspace_master_rw.py` after
finding the following — recording it here since it changes the actual scope
of both steps:

- **`processDecision` has an OnFleet side effect that isn't in `data_access.py`.**
  `docs/portal-dcc-rw.html` posts `action: "processDecision"` to GAS and reads
  `result.onfleetSuccess` / `result.onfleetMsg` back to show the contractor
  (see its JS around the fetch call). That means the live Apps Script
  `processDecision` triggers an OnFleet auto-assign (and apparently route
  creation) as part of handling an accept — not just a status write.
  `data_access.process_decision()` only flips `routes.status`. This is a
  second, separate OnFleet dependency beyond the `markFNAssigned` one step 7
  already calls out — worth deciding alongside it, since a portal endpoint
  built strictly around `data_access.process_decision()` would silently drop
  OnFleet auto-assignment on every accepted route.
- **`saveToFieldNation` also pushes to Monday.com.** `fn_utils.save_fn_to_sheet()`'s
  docstring/comments say the GAS-side `saveToFieldNation` action does "an
  inline Monday placeholder push (find by address + 2 mutations per matched
  item)" on top of the sheet write. `data_access.save_to_field_nation()` is a
  bare insert — it doesn't replicate that. Same category of gap as the two
  above: needs a home before this call site can be safely swapped.
- **`_cached_fetch_sent_records_from_sheet()` (≈380 lines, `tactical_workspace_master_rw.py`)
  is not a drop-in read swap.** It does a concurrent 6-tab fetch, per-row JSON
  parsing, live-vs-ghost route reconstruction against currently-live OnFleet
  task IDs, a migration-cutoff date filter, FN-posted/FN-provider hydration,
  and builds the per-task history log — all coupled tightly to the exact
  Sheet row/column shapes. Pointing it at `data_access.get_routes()` means
  re-deriving all of that from the `payload` JSONB column, which needs to be
  built and clicked through against a real staging database, not written
  blind.

Net effect: three of the write actions (`processDecision`, `saveToFieldNation`,
`markFNAssigned`) all have side effects in OnFleet and/or Monday.com that live
only in Apps Script today and aren't in this repo. Recommend deciding where
all three go (ported into Python, or kept as a small separate service called
from `data_access.py`) as one decision, using the actual GAS source as
reference — before wiring any of the write call sites. The read-only call
sites (`get_contractors`, `get_routes`) have no such hidden side effects and
are lower-risk to wire first, but `_cached_fetch_sent_records_from_sheet`
specifically still needs the careful port described above, ideally done
against a running staging instance rather than statically.

## Known data fixes applied during import

- `robert@niekotech.com` has two rows in the Sheet with different phone numbers; `import_from_sheets.py` forces the confirmed-correct one (`18186324368`) via `CONTRACTOR_FIELD_OVERRIDES` regardless of which row the Sheet lists last.
- Duplicate emails/work-order numbers caused by stray whitespace or genuine double-entry (e.g. `robert@niekotech.com` / `holmj777@yahoo.com` having a tab-prefixed twin, or `FN-Ruben Delgado-9/14` appearing twice in Accepted routes) are handled by the existing upsert-on-natural-key behavior — the later row in the Sheet wins and the duplicate is silently absorbed. Confirmed acceptable; no separate cleanup needed.

## Not covered here

- Normalizing `routes.locs` / `routes.stop_data` out of JSONB into their own tables — a reasonable later cleanup, not required for the migration itself.
- The unrestricted-IC allowlist: `import_from_sheets.py` ports the current hardcoded name list into `contractors.unrestricted` as a one-time seed, but the app's dispatch-filter code still needs updating to read that column instead of checking names in Python.

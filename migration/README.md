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
4. **Wire `data_access.py` into the app.** Swap each read/write call site in `tactical_workspace_master_rw.py` for the matching function here — see the migration doc's "Code changes required" section for the full action-to-function mapping. Keep the existing `@st.cache_data` decorators wrapping the new read functions so caching behavior doesn't change.
5. **Build the portal endpoint.** `docs/portal-dcc-rw.html` posts `processDecision` straight to GAS today and can't hold a database credential (it's public static HTML). It needs a small API endpoint in front of `data_access.process_decision()` — this is the one new piece of infrastructure in this migration, not a straight port.
6. **Repoint the portal and the Field Nation browser extension** at the new endpoint(s), and confirm with whoever owns the extension that its three bulk-provider actions are updated too.
7. **Decide on the `markFNAssigned` orchestration** (OnFleet route-plan rename, worker updates, Monday.com mutations) — it lives in Apps Script today and isn't replicated by `data_access.mark_fn_assigned()`, which only updates the row. Port it into Python or keep it as a small separate service; see `data_access.py`'s module docstring.
8. **Cut over.** Since this is a hard cutover: do the final verification pass against staging (row counts, spot-checked records, a live test session pointed at the new database) as the last gate, then switch the app's env vars and redeploy. Retire `GAS_WEB_APP_URL`, `IC_SHEET_URL`, and `DCC_SHARED_SECRET` once everything is confirmed live on the new backend.

## Known data fixes applied during import

- `robert@niekotech.com` has two rows in the Sheet with different phone numbers; `import_from_sheets.py` forces the confirmed-correct one (`18186324368`) via `CONTRACTOR_FIELD_OVERRIDES` regardless of which row the Sheet lists last.
- Duplicate emails/work-order numbers caused by stray whitespace or genuine double-entry (e.g. `robert@niekotech.com` / `holmj777@yahoo.com` having a tab-prefixed twin, or `FN-Ruben Delgado-9/14` appearing twice in Accepted routes) are handled by the existing upsert-on-natural-key behavior — the later row in the Sheet wins and the duplicate is silently absorbed. Confirmed acceptable; no separate cleanup needed.

## Not covered here

- Normalizing `routes.locs` / `routes.stop_data` out of JSONB into their own tables — a reasonable later cleanup, not required for the migration itself.
- The unrestricted-IC allowlist: `import_from_sheets.py` ports the current hardcoded name list into `contractors.unrestricted` as a one-time seed, but the app's dispatch-filter code still needs updating to read that column instead of checking names in Python.

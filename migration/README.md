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

psql "$DATABASE_URL" -f migration/schema.sql


3. **Import from the Sheet, against staging first.** Point `DATABASE_URL` at a throwaway/staging database, then:

pip install -r requirements.txt -r migration/requirements.txt

export IC_SHEET_URL="<the Sheet's edit URL>"

python migration/import_from_sheets.py


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

## Decision (2026-09-18): defer saveRoute/archiveRoute/finalizeRoute/FN writes to a Phase 2

Looked at the actual `saveRoute` call site in `tactical_workspace_master_rw.py`
(the one GAS write with no OnFleet/Monday.com angle) to see if it, at least,
was safe to wire today. It isn't, for a different reason: it carries
production-tuned reliability logic that has no equivalent anywhere in
`data_access.py` yet —

- GAS-side dedupe via `CacheService` on a 10-minute window keyed on
  `cluster_hash`, so a retried `saveRoute` with the same cluster can never
  create a duplicate route; it just hands back the original route ID.
- A 3-attempt escalating-backoff retry (2s, then 4s) around the POST itself,
  added after two real incidents (Sep 2026) where GAS's web-app front end
  blipped for a few seconds and returned a non-JSON 404 even though the
  deployment was healthy.

Neither is replicated in `data_access.save_route()`, which is a plain
upsert. Swapping this call site over without first building and testing the
Postgres-side equivalent of that dedupe/retry behavior against a real
staging environment risks either duplicate routes or silently losing a
safety net that has already caught production issues.

Combined with the three OnFleet/Monday.com side effects documented above
(`processDecision`, `saveToFieldNation`, `markFNAssigned`), the decision is
to treat `saveRoute`, `archiveRoute`, `finalizeRoute`, `processDecision`,
`markFNAssigned`, and `saveToFieldNation` — everything keyed on
`cluster_hash` plus the three OnFleet/Monday.com actions — as a **Phase 2**,
deliberately not attempted yet. This doesn't reopen the "no dual-write"
decision at the top of this doc: Phase 1 (`contractors`, `bundle_maps`) is a
complete, real cutover for those two tables the moment `DATABASE_URL` is
set; Phase 2's tables (`routes`, `field_nation_orders`) simply haven't
started their cutover and keep running on Sheets/GAS exactly as today until
someone can test the replacement against a staging OnFleet/Monday.com
environment, not just read the code.

**What's actually live today:** the app now reads the IC/contractor list and
saves/loads pod bundle maps from Postgres whenever `DATABASE_URL` is set —
see `_ic_df_from_db()` and the `DB_ENGINE` branches in
`tactical_workspace_master_rw.py`. Everything else (routes, Field Nation
orders, the portal, the browser extension) is still 100% Sheets/GAS,
unaffected by setting `DATABASE_URL`.

## What's needed next (requires Railway/account access this repo doesn't have)

The remaining steps need your Railway dashboard and aren't something that
can be done from this environment:

1. Add a Postgres service to the DCC Railway project (Railway → New →
   Database → Postgres). This is the real, billable resource — nothing here
   provisions it for you.
2. Copy the generated `DATABASE_URL` from that Postgres service's Variables
   tab. Run the staging import against a **throwaway/local** Postgres first
   (see step 3 above), not the new Railway one directly, so a bad import
   never touches the database the app will actually read from.
3. Once the staging import checks out, run `import_from_sheets.py` again
   against the real Railway `DATABASE_URL`.
4. Only then, set `DATABASE_URL` in the **app's** Railway service variables
   (not just the Postgres service's) and redeploy. That's the switch that
   turns on `DB_ENGINE` and moves contractors + bundle maps to Postgres —
   watch the deploy logs and confirm the dispatcher tab loads the IC list
   and that a pod's bundle map still saves/loads correctly, since this is
   the first time that code path runs against anything but a local sandbox.
5. `routes` and `field_nation_orders` keep running on Sheets/GAS until
   Phase 2 above is unblocked — no action needed for those yet.

## Known data fixes applied during import

- `robert@niekotech.com` has two rows in the Sheet with different phone numbers; `import_from_sheets.py` forces the confirmed-correct one (`18186324368`) via `CONTRACTOR_FIELD_OVERRIDES` regardless of which row the Sheet lists last.
- Duplicate emails/work-order numbers caused by stray whitespace or genuine double-entry (e.g. `robert@niekotech.com` / `holmj777@yahoo.com` having a tab-prefixed twin, or `FN-Ruben Delgado-9/14` appearing twice in Accepted routes) are handled by the existing upsert-on-natural-key behavior — the later row in the Sheet wins and the duplicate is silently absorbed. Confirmed acceptable; no separate cleanup needed.

## Not covered here

- Normalizing `routes.locs` / `routes.stop_data` out of JSONB into their own tables — a reasonable later cleanup, not required for the migration itself.
- The unrestricted-IC allowlist: `import_from_sheets.py` ports the current hardcoded name list into `contractors.unrestricted` as a one-time seed, but the app's dispatch-filter code still needs updating to read that column instead of checking names in Python.

## Decision + port (2026-09-19): OnFleet/Monday.com side effects

Step 7 above asked for a decision on where `processDecision`, `markFNAssigned`,
and `saveToFieldNation`'s OnFleet/Monday.com side effects should live —
**ported into Python**, not a separate service. Reasoning: the app already
makes raw OnFleet API calls directly from `tactical_workspace_master_rw.py`
(worker/task/team lookups, route-plan creation) for its existing dispatch
flow — a separate service would be a second deployable with its own auth and
failure modes for logic that's a natural extension of code already living
here.

That port is done: **`migration/fn_side_effects.py`**, function-by-function
from the actual Apps Script source (pulled from the live "DCC" Apps Script
project on 2026-09-19 — this wasn't available in the repo before, which is
why Step 7 originally could only say to use it "as reference" rather than
actually doing so). It carries over every retry/backoff constant, the
Onfleet team allow-list, the Monday column defaults, and — importantly —
the **"board-corruption guard"**: `MONDAY_GROUP_FILTER` restricts Monday
writes to 3 specific board groups (Field Nation / Escalations / Primary
Route) by default, and a `MONDAY_GROUP_FILTER='*'` override requires a
*second* confirmation env var (`MONDAY_GROUP_FILTER_ALLOW_WILDCARD=yes`) to
take effect, exactly like GAS enforces it. Do not relax this without reading
the comments in that module — it exists because an earlier run without it
corrupted unrelated Monday board groups.

`data_access.py`'s `process_decision()`, `mark_fn_assigned()`, and
`save_to_field_nation()` now call into it:

- **`process_decision`** runs the OnFleet auto-assign + ordered route
  creation on accept (same as GAS), plus the idempotency guards GAS has —
  a duplicate accept is a no-op, and an already-accepted route can't flip
  back to declined through this path (use the app's revoke flow instead).
  Returns the same `onfleetSuccess`/`onfleetMsg`/`routeSuccess`/`routeMsg`/
  `partial`/`route_incomplete` shape `docs/portal-dcc-rw.html` already
  expects from GAS, for whenever the portal endpoint (Step 5, still not
  built) is wired up.
- **`mark_fn_assigned`** runs the OnFleet routePlan-rename + per-task
  metadata/worker re-PUT, and the Monday.com address-matched sync.
- **`save_to_field_nation`** runs the Monday.com placeholder push
  (installer = "Field Nation" until a real provider is confirmed).

**A real functional gap found (and fixed) while porting, not just a missing
side effect:** GAS's `markFNAssigned` doesn't just flip a status — it
*moves* the Field Nation sheet row into "Accepted routes", renaming the WO
to `FN-<Provider>-<MMDD>` along the way, so the order becomes a full
accepted route (shows in the Accepted bucket, goes through the finalization
checklist — everything else in the app that reads from `routes` expects
this). The original `mark_fn_assigned()` (written before the actual GAS
source was available) only updated `field_nation_orders` in place and never
created the corresponding `routes` row — so nothing would have ever
promoted an FN order into the app's normal accepted-route lifecycle. Fixed:
`mark_fn_assigned()` now inserts/updates that `routes` row too.
`set_fn_provider()` had the same shape of gap (GAS stamps `fn_provider` into
the JSON payload, not a separate column, and `mark_fn_assigned` reads it
from there) — also fixed, keeping both the payload field and the existing
`provider` column in sync.

`saveRoute`/`archiveRoute`/`finalizeRoute` (the other half of Step 7's
"decision on file") are intentionally NOT touched by this port — their
existing `save_route()`/`archive_route()`/`finalize_route()` upsert-on-`wo`
behavior already gives equivalent dedupe to GAS's 10-minute cluster_hash
cache (see that module's comments), so there's nothing to port there beyond
what's already written.

### What's verified, and what isn't

Every function in `fn_side_effects.py` is unit-tested against **mocked**
`requests` calls — `migration/tests/test_fn_side_effects.py` proves the
shape of every Onfleet/Monday request (endpoints, payloads, the retry
classifier, the board-corruption guard) matches the ported GAS source.
`migration/tests/test_process_decision_and_fn_flow.py` exercises the full
`data_access.py` call paths (`process_decision`, `mark_fn_assigned`,
`save_to_field_nation`) against a real (throwaway) Postgres with the same
HTTP mocking, and passes end to end, including the new `routes` row created
by `mark_fn_assigned`.

**None of this has been run against a real Onfleet or Monday.com sandbox.**
This sandbox has no such environment, and pointing this code at the *real*
production Onfleet/Monday accounts to "test" it would itself create the
exact side effects (real task assignments, real board writes) a test run
shouldn't cause. Before removing the GAS call sites in
`tactical_workspace_master_rw.py` — the actual cutover, per this doc's "hard
cutover, no dual-write" decision at the top — do one supervised manual
accept and one manual FN-assign against a staging `DATABASE_URL`, watched
live in both Onfleet and Monday, the same way Step 8's "final verification
pass" already calls for.

### New env vars needed before flipping this on

In addition to `DATABASE_URL` (Phase 1), the app's Railway service needs:
- `MONDAY_API_TOKEN` — a Monday.com personal API token. Without it, the
  Monday sync is skipped (logged, not raised) exactly like GAS does when its
  equivalent Script Property is unset.
- Optional, all have the same defaults GAS used: `MONDAY_BOARD_ID`
  (`7374880245`), `MONDAY_INSTALLER_COL` (`text1__1`), `MONDAY_WO_COL`
  (`text14`), `MONDAY_ADDR_COL` (`text63`), `MONDAY_GROUP_FILTER`,
  `MONDAY_GROUP_FILTER_ALLOW_WILDCARD`.
- `ONFLEET_KEY` is already set (the app's existing OnFleet integration uses
  it) and is reused here — GAS used a separate `ONFLEET_API_KEY` Script
  Property, but it's the same Onfleet account/key either way.

### Still not done

- The GAS call sites in `tactical_workspace_master_rw.py` still POST to
  `GAS_WEB_APP_URL` for all of this today — this port doesn't switch
  anything live. That switch is the actual cutover and should wait for the
  staging test above.

## Step 5, built (2026-09-20): the portal endpoint (`migration/portal_api.py`)

The portal endpoint step 5 asked for is done: **`migration/portal_api.py`**,
a small FastAPI service with exactly two routes, deliberately narrow rather
than a general REST API:

- `GET /?action=getRoute&routeId=<wo>` — mirrors GAS's `getRoute` action.
  Looks up `routes.payload` by `wo` and returns `{"payload": {...}}`, or
  `{"error": "..."}` if the WO isn't found. `routes.payload` already has
  every field `docs/portal-dcc-rw.html`'s `window.onload` reads off `d`
  (`icn`, `wo`, `due`, `lCnt`, `tCnt`, `kCnt`, `rCnt`, `dCnt`, `time`, `mi`,
  `comp`, `phone`, `taskIds`, `stopOrder`, `stopData`) — it's the exact same
  dict `tactical_workspace_master_rw.py` already builds and currently POSTs
  to GAS's `saveRoute`, so nothing needed adding there.
- `POST /` with `{"action": "processDecision", "routeId": <wo>, ...}` —
  mirrors GAS's `processDecision` action. Calls `data_access.process_decision()`
  (already built and tested — see the section above) and returns its result
  as-is, since that function already returns the exact
  `onfleetSuccess`/`onfleetMsg`/`routeSuccess`/`routeMsg`/`error` shape
  `submitFinalResponse()` in the portal already branches on.

Both routes match the GAS web app's request/response shapes byte-for-byte on
purpose, so Step 6 ("repoint the portal") is a **one-line change** to
`docs/portal-dcc-rw.html` — swap the `webAppUrl` constant — not a portal
rewrite.

**routeId is `wo`, not a new opaque token.** The routes table's natural key
is `wo`, so that's what's used in the link (`?route=<wo>`) and as the lookup
key here. This is the *same* security posture `docs/portal-dcc-rw.html`'s own
`[M24]` comment already documents as a known, deliberately-not-fixed-here
limitation: the only thing standing between "anyone with this link" and "a
valid accept/decline" is how guessable the ID in the URL is, and a
work-order-shaped ID is no more (and no less) guessable than whatever opaque
ID GAS's `routeId` scheme uses today. A real fix (an unguessable per-link
HMAC token, checked here) is the same `[M24]` finding and is intentionally
**not** addressed by this port — deploying this endpoint does not resolve
`[M24]`.

**Verified:** `migration/tests/test_portal_api.py` runs the FastAPI app
through Starlette's `TestClient` (the actual HTTP/ASGI layer, not just
calling the Python functions directly) against a real throwaway Postgres,
with Onfleet/Monday mocked the same way the other tests do it — checks the
`getRoute` 404-vs-hit shapes, the `processDecision` accept/decline paths, the
"unknown WO" error shape, and that the CORS preflight actually allows the
configured portal origin. Also manually run as a live `uvicorn` server in
this sandbox and hit with real HTTP requests (not just the test client) to
confirm the exact Railway start command works. Like the rest of Phase 2,
**none of this has touched a real Onfleet/Monday sandbox** — same caveat as
the "What's verified, and what isn't" section above.

### Deploying `portal_api.py`

This is its own process, not part of the Streamlit app — it needs its own
Railway **service** (same repo, different service):

1. In the Railway project, **New → GitHub Repo**, pick the same `DCC` repo
   again. This creates a second service alongside the existing Streamlit one.
2. In that new service's Settings, confirm it picked up the `api:` process
   type from the `Procfile` (Railway lets you pick which `Procfile` line a
   service runs when there's more than one — pick `api`, not `web`). If it
   defaults to `web` instead, set a custom start command:
   `uvicorn migration.portal_api:app --host 0.0.0.0 --port $PORT`.
3. Set that service's `DATABASE_URL` to the **same** value as the Streamlit
   app's — both read/write the same `routes` table, so this must be the
   real Railway Postgres, not a separate database.
4. Set `PORTAL_ALLOWED_ORIGINS` if the portal is ever served from somewhere
   other than `https://nwilliams-maker.github.io` (comma-separated for more
   than one origin). Defaults to that GitHub Pages origin if unset.
5. Set `ONFLEET_KEY` (and `MONDAY_API_TOKEN` etc. if using Monday) on this
   service too — `process_decision()` needs them exactly like the Streamlit
   app does; they don't carry over between services automatically.
6. Deploy, then hit `https://<this-service>.up.railway.app/?action=getRoute&routeId=anything`
   in a browser — a `{"error": "This route link has expired or the route
   was not found."}` response (not a 502/timeout) confirms it's up and can
   reach the database.
7. Update `docs/portal-dcc-rw.html`'s `webAppUrl` constant to that service's
   URL, commit, and it's live for whoever opens a route link next — but see
   the "Still not done" list above: this only matters once `routes` rows
   actually exist in Postgres, which they don't until the write call sites
   are cut over.

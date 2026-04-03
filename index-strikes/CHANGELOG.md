# Changelog

## [Unreleased]

## [1.0.1] — 2026-04-04

### Fixed
- Fixed data refresh button

## [1.0.0] — 2026-04-04

### Added
- Sticky header with index identity band, refresh icon, and theme toggle
  in a unified icon-stack layout
- Responsive label abbreviations on mobile for toolbar toggles

### Fixed
- Dashboard: quantile form "Apply" no longer teleports to wrong view/tab when the
  user has JS-navigated since the last page load
- Dashboard: quantile form "Reset" link now preserves the current view/tab instead
  of always resetting to Strike Bounds / Daily
- MM Weekly expiry override: page no longer resets to Strike Bounds / Daily
  view after applying or resetting the override expiry date
- Quantile settings form: changing quantile values now preserves the active
  view and sub-tab instead of resetting to Strike Bounds / Daily
- Movement Frequency slider: fix input field overlapping slider track (flex layout)
- Weekly expiry weekday: weekday 0 (Monday) was silently replaced by Friday
  due to `or`-based falsy check; now uses explicit `is not None` guard
- Quantile slider disappearing when page loads on weekly tab (DOMContentLoaded
  relocation fix)
- Light-mode theme toggle visibility and stats tab reset on view switch

### Changed
- Replaced Minimum Movement Q2.5 table with interactive Movement Frequency
  slider (0–5%, 0.01% steps) showing how often a given move occurs per period
- Movement frequency computed client-side for instant slider response
- Replaced dual quantile number inputs with a symmetric tail-percentage slider
  (0–50%, auto-submit on release)
- Quantile slider relocated into tab content area (below section headers)
  instead of above the tab bar
- Unified daily/weekly Movement Frequency sliders into a single shared
  slider that relocates between tabs via DOM moves
- Redesigned toolbar: unified view and period toggles on a single line
  with segmented button design (Period on left, View on right)
- Return statistics section now toggles between daily and weekly context
  when switching Movement Frequency tabs
- Header aligned with main container at all viewport widths via Pico's
  `.container` class
- `compute_dashboard_data` now raises `ValueError` on empty DataFrame instead
  of returning a dict; callers catch the exception for cleaner type safety
- Promoted `_MIN_PERIOD_SAMPLES` and `_iter_year_periods` to public names
  (`MIN_PERIOD_SAMPLES`, `iter_year_periods`) in `statistics.py`
- Replaced `== True` SQLAlchemy anti-pattern with `.is_(True)`
- Added parameterized type annotations to `build_history_rows` and
  `_dists_to_dicts`
- Extracted inline f-string HTML in `refresh_symbol` endpoint to Jinja2
  partial (`partials/refresh_status.html`); fixed relative `hx-post` URL
  to absolute path

## [0.3.0] — 2026-04-01

### Added
- Minimum Movement section on the dashboard: shows the absolute minimum daily
  movement (Q2.5 quantile of absolute returns) for multiple history windows
  (30Y, 20Y, 10Y, Weekday 10Y), expressed as both a percentage and concrete
  up/down price levels from the last close.
- Weekly Minimum Movement section: same analysis using N-session returns
  (same window as the active weekly strike expiry), with 30Y, 20Y, and 10Y
  windows.
- Toolbar toggle to show/hide the Minimum Movement sections independently.
- `MinimumMovementResult` dataclass in `index_strikes/services/strikes.py`
  for structured result values.
- `compute_minimum_movement()` and `compute_weekly_minimum_movement()`
  calculation functions with automatic window selection based on data
  availability and a configurable quantile level.

### Changed
- Refactored strike computation: extracted named constants (`_MIN_PERIOD_SAMPLES`,
  `_FRIDAY`), deduplicated year-period iteration into a shared `_iter_year_periods`
  helper, introduced `DashboardData` dataclass with fully-parameterised type
  annotations, and decomposed `_compute_dashboard_data_sync` by extracting
  `_compute_weekly_strikes_data` and `_compute_weekly_stats_data` helpers.

## [0.2.2] — 2026-03-31
### Security
- Escape user-supplied symbol and exception text in refresh endpoint (XSS)

### Performance
- Chunked bulk INSERT in `save_market_data_to_db` (N→1 round trips, chunk size 7 000)

### Reliability
- Explicit async HTTP in `update_exchange_codes` via `httpx.AsyncClient`
- Migration retry attempts are now logged at WARNING level so failed DB
  connection attempts during startup are visible in the add-on log

### Fixed
- Dashboard and `/health` endpoint no longer show "Database unavailable" on
  slow DB connections — removed the hard 5-second `asyncio.wait_for` timeout
  that caused spurious failures when the database was reachable but responding
  slowly (e.g. during Alembic migrations or under load)

### Changed
- Return Statistics section now shows "(daily)" or "(N-session)" in its caption to
  match the active Strike Prices tab; switching to the Weekly tab shows the return
  distribution calculated from N-session returns (same N as the weekly strike prices).
  Manual expiry override updates the statistics automatically on reload.

## [0.2.1] — 2026-03-31
### Fixed
- Fix Daily/Weekly strike tabs both visible in HA ingress due to browser caching
  stale CSS/JS from prior version — static assets now include content-hash query
  parameters for automatic cache-busting on every deploy

## [0.2.0] — 2026-03-31
### Added
- Weekly strike price calculations based on N-session returns
- Exchange calendar integration (exchange_calendars) for accurate
  trading day counting
- Dashboard tabs: switch between Daily and Weekly strike views
- Manual expiry date override on the weekly strikes tab
- Per-index configuration: expiry weekday and exchange MIC code
- MIC code reference list with autocomplete and scheduled sync
  from ISO 10383 registry

## [0.1.37] — 2026-03-30
### Fixed
- Log timestamps now use the host's local timezone (e.g. CEST) instead of UTC
- Added optional `timezone` config option as fallback when Supervisor API detection is unavailable
- Prevented Alembic's `fileConfig()` from clobbering the root logger's timestamped formatter

## [0.1.36] — 2026-03-30
### Changed
- Weekday Volatility section now shows Q97.5/Q2.5 quantiles (previously Q95/Q5).
- Added interactive long-period selector (10Y / 20Y / 30Y) and short-period
  selector (90D / 180D / 360D) to the Weekday Volatility section.
- Long period options are hidden automatically when the index has insufficient
  history (requires ≥80% data coverage, same rule as Return Statistics blocks).
- Default periods are 20Y (long) and 180D (short).
- Period changes trigger an HTMX partial swap — only the weekday table updates.
- Return Statistics section redesigned: replaced the 10Y / 30D / 5D blocks with up to four long-horizon windows — 180D, 10Y, 20Y, 30Y. Year-based windows use exact calendar cutoffs (`pd.DateOffset`). Periods with less than 80% data coverage are excluded.
- Strike Prices header now shows today's date alongside the last known close date; a staleness warning appears when data is more than 3 days old.
- Strike prices now show multi-period analysis: 30Y, 20Y, 10Y, and Weekday 10Y
- Historical data fetch extended from 10 to 30 years for richer analysis
- Indices with limited history gracefully show only available periods
- Ingress path detection moved fully to per-request `X-Ingress-Path` header
  reading in `_IngressPathMiddleware`; the startup Supervisor API curl call
  and uvicorn `--root-path` flag have been removed from `run.sh`
- Add timestamped logging to all Python/uvicorn output (request logs, warnings, scheduler)

## [0.1.30] — 2026-03-29

### Fixed
- Replace bashio API calls with direct /data/options.json reading via jq —
  fixes "Unable to access the API, forbidden" errors that caused all config
  values (db_host, db_port, log_level, etc.) to be null
- Fix timezone detection to use SUPERVISOR_TOKEN + curl instead of bashio
- Fix ingress path reading (bashio::addon.ingress_path doesn't exist;
  now uses Supervisor REST API with graceful fallback)

### Changed
- Remove hassio_api/hassio_role from config.yaml (not needed for jq/curl approach)
- Add startup: application and boot: auto to config.yaml

## [0.1.29] — 2026-03-29

### Fixed
- Fix bashio config not being read in HA production — all user settings
  (db_host, db_port, db_user, db_password, update_schedule, log_level)
  were silently falling back to defaults because the bashio shell library
  was not sourced in run.sh
- Alembic migrations now connect to the user-configured database instead
  of localhost
- Log level and update schedule now respect user configuration

## 0.1.28 — 2026-03-29

### Fixed
- Fix dual process startup: remove s6 service from Dockerfile (only CMD remains)
- Fix double migration: remove synchronous Alembic from run.sh (lifespan thread handles it)
- Add diagnostic warning when DB_HOST is localhost/unset in production

### Improved
- Container logs now include date+time timestamps (YYYY-MM-DD HH:MM:SS)
- Container timezone auto-detected from HA system settings (no more UTC-only logs)

## 0.1.26 — 2026-03-29

### Fixed
- **Critical:** Fixed add-on startup failure when bashio config is missing or incomplete.
  The app now gracefully handles empty configuration values and provides clear error
  messages via `/health` endpoint instead of crashing.
- Improved configuration validation with field validators for `db_port` and `update_schedule`.
- Enhanced `/health` endpoint to report configuration validity and errors.
- Added troubleshooting guide to DOCS.md for common startup errors.

### Changed
- **Simplified Dockerfile**: removed COPY heredocs and runtime patch script in
  favour of standard COPY commands (builds now run on GitHub Actions, not
  local HA Docker-in-Docker)
- `run.sh` now logs which configuration source is being used (bashio, options.json, or env vars).
- Empty environment variables are no longer passed to Pydantic; defaults are used instead.

## 0.1.25 — 2026-03-14

### Changed
- **Index sorting replaced with ↑/↓ buttons**: drag-and-drop was unreliable
  under HA ingress URL rewriting. Each row in the "Configured Indices" table
  now has up/down arrow buttons; the ↑ button on the first row and ↓ on the
  last are disabled. Clicking a button persists the new order immediately and
  re-renders the table with correct disabled states.

## 0.1.24 — 2026-03-14

### Added
- **Drag-and-drop sorting for the Manage Indices page**: rows in the
  "Configured Indices" table can now be reordered by dragging the `⋮` handle.
  The new order is persisted immediately via `POST /indices/reorder` and
  carries through to the index-selector dropdown on the dashboard.

## 0.1.23 — 2026-03-14

### Fixed
- **Historical Data table was empty on initial load, showing stale rows after "Load More"**:
  `history_rows.html` expects a top-level `history_rows` template variable, but the
  dashboard routes only provided it nested inside the `data` dict. The for-loop therefore
  produced no rows on the initial render, while the "Load More" HTMX button started at
  offset 30 — skipping the 30 most-recent rows and fetching the *next* 30 instead (which
  happened to be late-January data). Fixed by adding `history_rows` as a top-level context
  variable in both `routers/dashboard.py` and `routers/api.py`.

### Changed
- **Strike prices table**: percentage values are now shown directly below the price in the
  same column (High Strike / Low Strike), removing the separate "High %" and "Low %" columns.

## 0.1.22 — 2026-03-10

### Fixed
- **Strikes calculated from intraday price instead of confirmed last close**: yfinance
  returns the current live price as `Close` for today's bar while the market is open.
  All fetches are now capped at yesterday (`date.today() - 1`) so partial intraday bars
  are never stored in the database or used as the basis for strike calculations.

## 0.1.13 — 2026-03-07

### Fixed
- **502 Bad Gateway on first request**: Database migrations were running in a
  background daemon thread, creating a race condition where requests arrived
  before tables were created. Migrations now block startup (with 30-second
  timeout) to ensure the database schema exists before accepting requests.
- Added 10-second connection timeout to asyncpg to fail fast if database is
  unreachable (prevents app hanging on unreachable connections).
- Enhanced `/health` endpoint to verify actual database connectivity instead
  of just returning a static "ok" response.

## 0.1.6 — 2026-03-07

### Fixed
- **Root cause found and fixed**: created `/etc/services.d/addon/run` in the
  Dockerfile so s6-overlay actually starts `/run.sh`. The HA base image
  `aarch64-base-python:3.12-alpine3.18` uses s6-overlay v3 and leaves
  `/etc/services.d/` completely empty; older base image versions shipped this
  file pre-built. Without it the "legacy-services" bundle started successfully
  but with nothing to supervise, so `/run.sh` was never executed and port 8099
  was never bound — this was the true cause of all prior "Cannot connect"
  errors from v0.1.0 onwards.

## 0.1.5 — 2026-03-07

### Fixed
- Removed `exec 1>/proc/1/fd/1 2>/proc/1/fd/2` from run.sh: in HA base images
  with s6-overlay v3, PID 1's fd 1 may be closed or pointed at /dev/null after
  init; opening `/proc/1/fd/1` for writing then blocks indefinitely (FIFO with
  no reader), causing a silent 5-minute hang before the first ingress error.
- Added `set -x` to run.sh: bash now traces every command to stderr before
  executing it; s6-overlay captures stderr and forwards it to the HA add-on log
  tab, providing a full command-by-command execution trail for diagnosis.
- Added `--max-time 5` to the `curl` call for the supervisor ingress-path API
  so a slow or unresponsive supervisor cannot hang startup indefinitely.

## 0.1.4 — 2026-03-07

### Fixed
- Port 8099 was never bound because `#!/usr/bin/with-bashio` exited silently
  under `set -e` before reaching the `exec uvicorn` line:
  - Replaced with `#!/bin/bash`; add-on options are now read directly from
    `/data/options.json` using `jq` (same source bashio reads), eliminating
    the bashio dependency in run.sh entirely.
  - Ingress path is fetched from the supervisor REST API via `curl` with a
    graceful fallback to `""` if the call fails.
- Add-on log output (including Python tracebacks) was invisible in the HA log
  tab because s6-overlay v3 routes supervised-service stdout through its own
  log pipeline rather than the container's main stdout:
  - Added `exec 1>/proc/1/fd/1 2>/proc/1/fd/2` at the top of run.sh so all
    output is written to PID 1's file descriptors, which Docker captures.
- Reverted alembic migrations to asyncpg (psycopg2-binary can fail to load on
  Alpine musl libc); added `connect_args={"timeout": 5}` to asyncpg engine so
  an unreachable DB fails in 5 s instead of waiting for the OS TCP timeout.
- Migrations now run as an `asyncio.create_task` background task so uvicorn
  binds port 8099 immediately after Python finishes importing (~10-20 s on ARM
  Alpine) rather than waiting for the DB connection attempt first.
- Added `jq` and `curl` to the Alpine package install.
- Removed `psycopg2-binary` from pip install (replaced by asyncpg for all DB
  operations including migrations).

## 0.1.3 — 2026-03-07

### Fixed
- Add-on startup took ~45 seconds before port 8099 was open, causing HA to
  show the "not ready yet" popup every time the UI was opened:
  - `alembic/env.py`: switched from async asyncpg to sync psycopg2 with a
    5-second `connect_timeout`; unreachable DB now fails in 5 s instead of
    waiting ~25 s for the OS TCP timeout.
  - `main.py`: added `asyncio.wait_for(..., timeout=15)` around the migration
    call as a hard upper-bound safety net.
  - `Dockerfile` / run.sh: removed the pre-flight `python3 -c "import ..."`
    check that was importing the whole app twice (~15 s extra on ARM Alpine).
- Added a plain `echo` probe at the very top of run.sh (before any bashio
  call) to verify run.sh is executing and stdout is captured in HA logs.

## 0.1.2 — 2026-03-07

### Fixed
- Container starts but nothing listens on port 8099: `uvicorn[standard]` pulls
  in `uvloop` (a C event-loop extension) which segfaults silently on Alpine
  aarch64, causing a s6-overlay crash/restart loop with no Python traceback.
  Fixed by switching to plain `uvicorn` and passing `--loop asyncio` so the
  pure-Python asyncio event loop is used instead.
- Added `ENV PYTHONUNBUFFERED=1` to the Dockerfile and a pre-flight
  `python3 -c "from index_strikes.main import app"` in the run script so any
  future import errors appear in the add-on logs rather than disappearing.

## 0.1.1 — 2026-03-07

### Fixed
- Container startup failure: `/run.sh: not found` caused by a stale Docker
  buildx layer cache skipping the `COPY run.sh` instruction. The entrypoint
  script is now embedded inline in the Dockerfile via a BuildKit heredoc so
  it is always present regardless of build-cache state.

## 0.1.0 — 2026-03-05

### Added
- Initial release as a Home Assistant add-on
- Dashboard with strike price recommendations for configured stock indices
- Daily automatic data updates via APScheduler
- Manual refresh button per index
- Index management UI (add/remove/toggle indices)
- Historical data table with breach highlighting
- Statistics panels (10Y, 30D, 5D)
- Weekday volatility comparison
- PostgreSQL persistence with automatic Alembic migrations
- HA ingress support with correct base path handling
- Support for aarch64 (Raspberry Pi 5) and amd64 architectures

import os
import sys
import json
import time
import csv
import io
import base64
import signal
import logging
import requests
import threading
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from collections import defaultdict

from google.oauth2 import service_account
from googleapiclient.discovery import build
from flask import Flask, jsonify, request

# ==========================================================
# LOGGING — structured, goes to Render's log stream
# ==========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

# ==========================================================
# ENV VALIDATION — fail with a clear message, not a traceback
# ==========================================================
def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        log.error(f"FATAL: Missing required env var: {name}")
        # Don't sys.exit here — let Flask start and report the error via /health
    return val or ""

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

ACCOUNT_SID  = _require_env("IMPACT_ACCOUNT_SID")
AUTH_TOKEN   = _require_env("IMPACT_AUTH_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "")

AUTH = HTTPBasicAuth(ACCOUNT_SID, AUTH_TOKEN) if ACCOUNT_SID and AUTH_TOKEN else None

# ==========================================================
# CONFIG
# ==========================================================
IMPACT_BASE_URL          = "https://api.impact.com"
SHEET_MAIN_AGGREGATE     = "Sheet1"
CHUNK_DAYS               = 30
PAGE_SIZE                = 5000
RATE_LIMIT_DELAY         = 0.5
CLICK_POLL_INTERVAL      = 5
CLICK_MAX_POLLS          = 60
CLICK_BETWEEN_CAMPAIGNS  = 30
CLICK_429_MAX_RETRIES    = 5
CLICK_429_BASE_WAIT      = 60
CLICK_429_MAX_WAIT       = 120
MAX_PAGES_PER_CHUNK      = 200   # safety cap — prevents infinite pagination loops

# ==========================================================
# SYNC STATE — prevent concurrent runs and expose status
# ==========================================================
_sync_lock   = threading.Lock()
_sync_status = {
    "running":    False,
    "last_start": None,
    "last_end":   None,
    "last_error": None,
    "rows_synced": 0,
}

# ==========================================================
# GRACEFUL SHUTDOWN
# ==========================================================
_shutdown_event = threading.Event()

def _handle_signal(signum, frame):
    log.info(f"Received signal {signum}. Setting shutdown flag.")
    _shutdown_event.set()

signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)

# ==========================================================
# FLASK APP
# ==========================================================
app = Flask(__name__)

@app.route("/")
def index():
    return jsonify({"status": "ok", "service": "Impact Data Sync"})

@app.route("/health")
def health():
    missing = []
    if not ACCOUNT_SID:  missing.append("IMPACT_ACCOUNT_SID")
    if not AUTH_TOKEN:   missing.append("IMPACT_AUTH_TOKEN")
    if not SPREADSHEET_ID: missing.append("SPREADSHEET_ID")

    if missing:
        return jsonify({"status": "unhealthy", "missing_env": missing}), 500

    status = dict(_sync_status)
    status["status"] = "healthy"
    return jsonify(status), 200

@app.route("/trigger", methods=["POST"])
def trigger_sync():
    if _sync_status["running"]:
        return jsonify({"status": "already_running", "message": "A sync is already in progress."}), 409

    full_refresh = request.args.get("full", "false").lower() == "true"
    skip_clicks  = request.args.get("skip_clicks", "false").lower() == "true"

    thread = threading.Thread(
        target=_run_sync_safe, args=(full_refresh, skip_clicks), daemon=True
    )
    thread.start()
    return jsonify({"status": "sync started", "full_refresh": full_refresh, "skip_clicks": skip_clicks}), 202

@app.route("/trigger/full", methods=["POST"])
def trigger_full_sync():
    if _sync_status["running"]:
        return jsonify({"status": "already_running"}), 409

    thread = threading.Thread(target=_run_sync_safe, args=(True, False), daemon=True)
    thread.start()
    return jsonify({"status": "full sync started"}), 202

@app.route("/status")
def sync_status():
    return jsonify(dict(_sync_status))

# ==========================================================
# SYNC RUNNER — wraps main() with state tracking
# ==========================================================
def _run_sync_safe(full_refresh: bool = False, skip_clicks: bool = False):
    """Thread-safe wrapper around main(). Updates _sync_status."""
    if not _sync_lock.acquire(blocking=False):
        log.warning("Sync already running — skipping this trigger.")
        return

    _sync_status["running"]    = True
    _sync_status["last_start"] = datetime.utcnow().isoformat()
    _sync_status["last_error"] = None

    try:
        rows = main(full_refresh=full_refresh, skip_clicks=skip_clicks)
        _sync_status["rows_synced"] = rows
        _sync_status["last_end"]    = datetime.utcnow().isoformat()
        log.info(f"✅ Sync completed. Rows synced: {rows}")
    except Exception as e:
        _sync_status["last_error"] = str(e)
        _sync_status["last_end"]   = datetime.utcnow().isoformat()
        log.error(f"❌ Sync failed: {e}", exc_info=True)
    finally:
        _sync_status["running"] = False
        _sync_lock.release()

# ==========================================================
# GOOGLE SHEETS HELPERS
# ==========================================================
def get_google_creds():
    b64 = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if b64:
        try:
            # Support both raw JSON and base64-encoded JSON
            try:
                creds_json = json.loads(b64)
            except json.JSONDecodeError:
                creds_json = json.loads(base64.b64decode(b64))
            return service_account.Credentials.from_service_account_info(
                creds_json,
                scopes=["https://www.googleapis.com/auth/spreadsheets"],
            )
        except Exception as e:
            raise RuntimeError(f"Failed to parse GOOGLE_CREDENTIALS_JSON: {e}") from e

    if os.path.exists("credentials.json"):
        return service_account.Credentials.from_service_account_file(
            "credentials.json",
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )

    raise RuntimeError(
        "No Google credentials found. Set GOOGLE_CREDENTIALS_JSON env var "
        "or provide credentials.json."
    )

def get_sheets_service():
    return build("sheets", "v4", credentials=get_google_creds())

# ==========================================================
# DATE HELPERS
# ==========================================================
def parse_iso_date(iso_str: str) -> str:
    try:
        return iso_str.split("T")[0] if "T" in iso_str else iso_str
    except Exception:
        return iso_str

def get_last_synced_date(service):
    """Read Sheet1 column A to find the most recent date already synced."""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_MAIN_AGGREGATE}'!A:A",
        ).execute()
        values = result.get("values", [])
        if len(values) <= 1:
            return None

        latest = None
        for row in values[1:]:
            if row and row[0]:
                try:
                    d = datetime.strptime(row[0], "%Y-%m-%d").date()
                    if latest is None or d > latest:
                        latest = d
                except Exception:
                    pass
        return latest
    except Exception as e:
        log.warning(f"Could not read last synced date: {e}")
        return None

def get_date_chunks(start_date, end_date):
    chunks = []
    cur = start_date
    while cur < end_date:
        nxt = min(cur + timedelta(days=CHUNK_DAYS), end_date)
        chunks.append((
            f"{cur.strftime('%Y-%m-%d')}T00:00:00-08:00",
            f"{nxt.strftime('%Y-%m-%d')}T00:00:00-08:00",
        ))
        cur = nxt + timedelta(days=1)
    return chunks

# ==========================================================
# HTTP HELPER — retries with exponential back-off
# ==========================================================
def _get_with_retry(url, params=None, max_attempts=5, timeout=60, label="request"):
    """
    GET with retry on 429 / transient errors.
    Returns (response | None). Never raises.
    """
    for attempt in range(1, max_attempts + 1):
        if _shutdown_event.is_set():
            log.info("Shutdown requested — aborting HTTP request.")
            return None
        try:
            resp = requests.get(
                url, params=params, auth=AUTH,
                headers={"Accept": "application/json"}, timeout=timeout,
            )
            if resp.status_code == 200:
                return resp
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                wait = min(int(retry_after) if retry_after else 60 * attempt, 300)
                log.warning(f"[{label}] 429 rate limited. Waiting {wait}s (attempt {attempt}/{max_attempts})...")
                time.sleep(wait)
                continue
            log.warning(f"[{label}] HTTP {resp.status_code}: {resp.text[:200]}")
            return None
        except requests.exceptions.Timeout:
            log.warning(f"[{label}] Timeout (attempt {attempt}/{max_attempts})")
        except Exception as e:
            log.warning(f"[{label}] Exception (attempt {attempt}/{max_attempts}): {e}")
        time.sleep(2 ** attempt)

    log.error(f"[{label}] All {max_attempts} attempts failed.")
    return None

# ==========================================================
# FETCH CAMPAIGNS
# ==========================================================
def fetch_campaigns():
    log.info("Fetching campaign list...")
    resp = _get_with_retry(
        f"{IMPACT_BASE_URL}/Advertisers/{ACCOUNT_SID}/Campaigns",
        label="campaigns",
    )
    if not resp:
        return []

    data = resp.json()
    campaigns = data.get("Campaigns", []) or data.get("Records", [])
    log.info(f"Found {len(campaigns)} campaigns.")
    return campaigns

# ==========================================================
# FETCH ACTIONS (Conversions) — Chunked + page-capped
# ==========================================================
def fetch_actions(campaigns, date_chunks):
    log.info(f"Fetching Actions in {len(date_chunks)} chunk(s)...")
    actions_data = defaultdict(lambda: {"actions": 0, "revenue": 0.0, "cost": 0.0})

    for campaign in campaigns:
        if _shutdown_event.is_set():
            log.info("Shutdown — stopping actions fetch early.")
            break

        campaign_id   = campaign.get("Id")
        campaign_name = campaign.get("Name")
        log.info(f"  Actions: {campaign_name} ({campaign_id})")

        for chunk_start, chunk_end in date_chunks:
            if _shutdown_event.is_set():
                break

            page = 1
            while page <= MAX_PAGES_PER_CHUNK:
                if _shutdown_event.is_set():
                    break

                resp = _get_with_retry(
                    f"{IMPACT_BASE_URL}/Advertisers/{ACCOUNT_SID}/Actions",
                    params={
                        "CampaignId": campaign_id,
                        "ActionDateStart": chunk_start,
                        "ActionDateEnd": chunk_end,
                        "Page": page,
                        "PageSize": PAGE_SIZE,
                        "Format": "JSON",
                    },
                    label=f"actions/{campaign_id}/p{page}",
                )

                if not resp:
                    break

                data    = resp.json()
                actions = data.get("Actions", []) or data.get("Records", [])
                if not actions:
                    break

                for action in actions:
                    date_str     = parse_iso_date(action.get("EventDate", ""))
                    partner_name = action.get("MediaPartnerName", "Unknown")
                    revenue      = float(action.get("Amount", 0) or 0)
                    payout       = float(action.get("Payout", 0) or 0)

                    key = (date_str, campaign_name, partner_name)
                    actions_data[key]["actions"] += 1
                    actions_data[key]["revenue"] += revenue
                    actions_data[key]["cost"]    += payout

                if not data.get("@nextpageuri") or len(actions) < PAGE_SIZE:
                    break

                page += 1
                time.sleep(RATE_LIMIT_DELAY)

            if page > MAX_PAGES_PER_CHUNK:
                log.warning(f"  Hit MAX_PAGES_PER_CHUNK ({MAX_PAGES_PER_CHUNK}) for {campaign_name}. Some data may be missing.")

    return actions_data

# ==========================================================
# FETCH CLICKS (Async ClickExport)
# ==========================================================
def submit_click_job(campaign_id, start_date, end_date):
    url = f"{IMPACT_BASE_URL}/Advertisers/{ACCOUNT_SID}/Programs/{campaign_id}/ClickExport"

    for retry in range(CLICK_429_MAX_RETRIES):
        if _shutdown_event.is_set():
            return None

        resp = _get_with_retry(
            url,
            params={"DateStart": start_date, "DateEnd": end_date},
            max_attempts=1,           # _get_with_retry handles 429 internally
            label=f"click_submit/{campaign_id}",
        )
        if resp:
            queued_uri = resp.json().get("QueuedUri", "")
            if "/Jobs/" in queued_uri:
                return queued_uri.split("/Jobs/")[1].split("/")[0]
            return None

        # If _get_with_retry already exhausted its attempts, give up
        log.warning(f"  submit_click_job failed after retries for {campaign_id}.")
        return "RATE_LIMITED"

    return None

def fetch_clicks(campaigns, start_date, end_date):
    log.info("Fetching Clicks via ClickExport...")
    clicks_data = defaultdict(lambda: {"clicks": 0})

    for i, campaign in enumerate(campaigns):
        if _shutdown_event.is_set():
            log.info("Shutdown — stopping clicks fetch early.")
            break

        campaign_id   = campaign.get("Id")
        campaign_name = campaign.get("Name")
        log.info(f"  Clicks [{i+1}/{len(campaigns)}]: {campaign_name}")

        if i > 0:
            log.info(f"  Waiting {CLICK_BETWEEN_CAMPAIGNS}s between campaigns...")
            # Interruptible sleep
            for _ in range(CLICK_BETWEEN_CAMPAIGNS):
                if _shutdown_event.is_set():
                    break
                time.sleep(1)

        try:
            job_id = submit_click_job(campaign_id, start_date, end_date)
            if job_id == "RATE_LIMITED":
                log.warning("ClickExport rate-limited. Skipping remaining campaigns.")
                break
            if not job_id:
                log.warning(f"  No job ID returned for {campaign_name}. Skipping.")
                continue

            log.info(f"  Job ID: {job_id}")
            job_url = f"{IMPACT_BASE_URL}/Advertisers/{ACCOUNT_SID}/Jobs/{job_id}"
            completed = False

            for poll in range(CLICK_MAX_POLLS):
                if _shutdown_event.is_set():
                    break
                time.sleep(CLICK_POLL_INTERVAL)

                resp = _get_with_retry(job_url, label=f"job_poll/{job_id}")
                if not resp:
                    continue

                status = resp.json().get("Status", "UNKNOWN")
                log.info(f"  Poll {poll+1}: {status}")
                if status == "COMPLETED":
                    completed = True
                    break
                if status in ("FAILED", "CANCELLED"):
                    log.warning(f"  Job {status}.")
                    break

            if not completed:
                log.warning(f"  Job did not complete in time for {campaign_name}. Skipping.")
                continue

            download_url = f"{IMPACT_BASE_URL}/Advertisers/{ACCOUNT_SID}/Jobs/{job_id}/Download"
            dl_resp = requests.get(download_url, auth=AUTH, timeout=120)
            if dl_resp.status_code != 200:
                log.warning(f"  Download failed: {dl_resp.status_code}")
                continue

            reader = csv.DictReader(io.StringIO(dl_resp.text))
            rows = list(reader)
            log.info(f"  {len(rows)} click rows downloaded.")

            for row in rows:
                date_str     = parse_iso_date(row.get("EventDate", ""))
                partner_name = row.get("MediaName", "Unknown")
                key = (date_str, campaign_name, partner_name)
                clicks_data[key]["clicks"] += 1

        except Exception as e:
            log.error(f"  Error processing clicks for {campaign_name}: {e}", exc_info=True)
            continue

    return clicks_data

# ==========================================================
# MERGE DATA
# ==========================================================
def merge_data(actions_data, clicks_data):
    all_keys  = set(actions_data.keys()) | set(clicks_data.keys())
    rows      = []
    brand_rows = defaultdict(list)

    for key in sorted(all_keys, key=lambda x: x[0], reverse=True):
        date_str, campaign_name, partner_name = key
        a = actions_data.get(key, {"actions": 0, "revenue": 0.0, "cost": 0.0})
        c = clicks_data.get(key, {"clicks": 0})

        actions     = a["actions"]
        revenue     = round(a["revenue"], 2)
        total_cost  = round(a["cost"], 2)
        clicks      = c["clicks"]
        cpc         = round(total_cost / clicks, 2) if clicks > 0 else 0.0

        row = [date_str, campaign_name, partner_name, clicks, actions, revenue, total_cost, total_cost, cpc]
        rows.append(row)
        brand_rows[campaign_name.replace(":", "").replace("/", "-")[:30]].append(row)

    log.info(f"Merged {len(rows)} unique rows.")
    return rows, brand_rows

# ==========================================================
# GOOGLE SHEETS WRITE — safe & retried
# ==========================================================
def _sheets_call_with_retry(call_fn, label="sheets", max_attempts=4):
    """Execute a Google Sheets API call with exponential back-off."""
    for attempt in range(1, max_attempts + 1):
        try:
            return call_fn()
        except Exception as e:
            err = str(e)
            if "quota" in err.lower() or "rate" in err.lower():
                wait = 30 * attempt
                log.warning(f"[{label}] Quota/rate error (attempt {attempt}). Waiting {wait}s...")
                time.sleep(wait)
            else:
                log.error(f"[{label}] Sheets error (attempt {attempt}): {e}")
                if attempt == max_attempts:
                    raise
                time.sleep(5 * attempt)
    raise RuntimeError(f"[{label}] Failed after {max_attempts} attempts.")

def ensure_sheet_exists(service, sheet_title):
    try:
        metadata = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        existing = [s["properties"]["title"] for s in metadata.get("sheets", [])]
        if sheet_title not in existing:
            log.info(f"Creating sheet: {sheet_title}")
            service.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={"requests": [{"addSheet": {"properties": {"title": sheet_title}}}]},
            ).execute()
            return True
        return False
    except Exception as e:
        log.warning(f"ensure_sheet_exists error: {e}")
        return False

def sheet_has_data(service, sheet_title):
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{sheet_title}'!A1",
        ).execute()
        return bool(result.get("values"))
    except Exception:
        return False

def clear_all_sheets(service):
    """
    Delete all non-Sheet1 sheets, then clear Sheet1.
    Runs inside a try/except so a partial failure doesn't abort the sync.
    """
    log.info("Clearing all existing sheets for full refresh...")
    try:
        metadata = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        sheets   = metadata.get("sheets", [])

        delete_requests = [
            {"deleteSheet": {"sheetId": s["properties"]["sheetId"]}}
            for s in sheets
            if s["properties"]["title"] != SHEET_MAIN_AGGREGATE
        ]

        if delete_requests:
            service.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={"requests": delete_requests},
            ).execute()

        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_MAIN_AGGREGATE}'!A:Z",
        ).execute()
        log.info("All sheets cleared.")
    except Exception as e:
        log.error(f"clear_all_sheets error: {e}. Proceeding without clearing.")

def sync_to_sheets(master_rows, brand_rows, full_refresh):
    service = get_sheets_service()
    svc     = service.spreadsheets()

    header = [["DATE", "CAMPAIGN", "PARTNER", "CLICKS",
               "ACTIONS", "REVENUE USD", "ACTIONS COST USD", "TOTAL COST USD", "CPC USD"]]

    if full_refresh:
        clear_all_sheets(service)

    log.info(f"Syncing {len(master_rows)} rows to '{SHEET_MAIN_AGGREGATE}'...")

    if full_refresh or not sheet_has_data(service, SHEET_MAIN_AGGREGATE):
        _sheets_call_with_retry(lambda: svc.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_MAIN_AGGREGATE}'!A1",
            valueInputOption="RAW",
            body={"values": header + master_rows},
        ).execute(), label=SHEET_MAIN_AGGREGATE)
    else:
        _sheets_call_with_retry(lambda: svc.values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_MAIN_AGGREGATE}'!A:I",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": master_rows},
        ).execute(), label=SHEET_MAIN_AGGREGATE)

    for sheet_name, data_rows in brand_rows.items():
        if _shutdown_event.is_set():
            log.info("Shutdown — stopping brand sheet writes.")
            break

        log.info(f"  Writing '{sheet_name}': {len(data_rows)} rows")
        is_new = ensure_sheet_exists(service, sheet_name)

        if full_refresh or is_new:
            _sheets_call_with_retry(lambda sn=sheet_name, dr=data_rows: svc.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{sn}'!A1",
                valueInputOption="RAW",
                body={"values": header + dr},
            ).execute(), label=sheet_name)
        else:
            _sheets_call_with_retry(lambda sn=sheet_name, dr=data_rows: svc.values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{sn}'!A:I",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": dr},
            ).execute(), label=sheet_name)

# ==========================================================
# MAIN SYNC LOGIC
# ==========================================================
def main(full_refresh: bool = False, skip_clicks: bool = False) -> int:
    """
    Runs one full sync cycle.
    Args come from callers only — sys.argv is never read here.
    Returns the number of rows synced.
    """
    log.info(f"Impact Data Sync | mode={'FULL' if full_refresh else 'INCREMENTAL'} | skip_clicks={skip_clicks}")

    if not AUTH:
        raise RuntimeError("Cannot sync: missing IMPACT_ACCOUNT_SID or IMPACT_AUTH_TOKEN.")

    service = get_sheets_service()

    if full_refresh:
        start_date = (datetime.today() - timedelta(days=1095)).date()
        log.info(f"Full refresh from: {start_date}")
    else:
        last_date = get_last_synced_date(service)
        if last_date:
            start_date = last_date
            log.info(f"Incremental from last synced date: {start_date}")
        else:
            # No existing data — do a full refresh but DON'T silently flip the flag
            start_date = (datetime.today() - timedelta(days=1095)).date()
            full_refresh = True
            log.info(f"No existing data found. Starting full refresh from: {start_date}")

    end_date = datetime.today().date()

    if start_date >= end_date:
        log.info("Already up to date. Nothing to sync.")
        return 0

    log.info(f"Date range: {start_date} → {end_date}")

    campaigns = fetch_campaigns()
    if not campaigns:
        raise RuntimeError("No campaigns returned from Impact API. Aborting.")

    date_chunks  = get_date_chunks(start_date, end_date)
    actions_data = fetch_actions(campaigns, date_chunks)

    if skip_clicks:
        log.info("Skipping clicks (skip_clicks=True).")
        clicks_data = {}
    else:
        clicks_data = fetch_clicks(
            campaigns,
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
        )

    master_rows, brand_rows = merge_data(actions_data, clicks_data)

    if not master_rows:
        log.info("No new data to sync.")
        return 0

    sync_to_sheets(master_rows, brand_rows, full_refresh)
    log.info(f"🎉 Sync complete — {len(master_rows)} rows.")
    return len(master_rows)

# ==========================================================
# ENTRY POINT
# ==========================================================
if __name__ == "__main__":
    # Parse CLI args here — never inside main()
    cli_full_refresh = "--full"        in sys.argv
    cli_skip_clicks  = "--skip-clicks" in sys.argv
    cli_no_server    = "--no-server"   in sys.argv

    if cli_no_server:
        # Useful for one-off manual runs (e.g. local testing)
        log.info("Running in CLI mode (no Flask server).")
        try:
            rows = main(full_refresh=cli_full_refresh, skip_clicks=cli_skip_clicks)
            sys.exit(0 if rows >= 0 else 1)
        except Exception as e:
            log.error(f"Sync failed: {e}", exc_info=True)
            sys.exit(1)

    # Start initial sync in background BEFORE Flask binds the port.
    # This way Render's health check passes immediately.
    init_thread = threading.Thread(
        target=_run_sync_safe,
        args=(cli_full_refresh, cli_skip_clicks),
        daemon=True,
    )
    init_thread.start()

    port = int(os.environ.get("PORT", 10000))
    log.info(f"Starting Flask on 0.0.0.0:{port}")

    # use_reloader=False is critical — reloader forks the process and
    # double-starts the sync thread, causing data corruption on Render.
    app.run(host="0.0.0.0", port=port, use_reloader=False, threaded=True)

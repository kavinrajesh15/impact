import os
import sys
import json
import time
import csv
import io
import base64
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from collections import defaultdict

from google.oauth2 import service_account
from googleapiclient.discovery import build
from flask import Flask
import threading
# ==========================================================
# CONFIG
# ==========================================================
IMPACT_BASE_URL = "https://api.impact.com"
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1sAc23AM5vdCfKOCxwI-vbM0lCdk8zRDW-iGbB_wkhM0")
SHEET_MAIN_AGGREGATE = "Sheet1"

CHUNK_DAYS = 30
PAGE_SIZE = 5000
RATE_LIMIT_DELAY = 0.5
CLICK_POLL_INTERVAL = 5
CLICK_MAX_POLLS = 60
CLICK_BETWEEN_CAMPAIGNS = 30
CLICK_429_MAX_RETRIES = 5
CLICK_429_BASE_WAIT = 60
CLICK_429_MAX_WAIT = 120

# ==========================================================
# LOAD CREDENTIALS
# ==========================================================
# Load .env file for local dev (silently skip on Render)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

ACCOUNT_SID = os.getenv("IMPACT_ACCOUNT_SID")
AUTH_TOKEN = os.getenv("IMPACT_AUTH_TOKEN")

if not ACCOUNT_SID or not AUTH_TOKEN:
    raise Exception("Missing IMPACT_ACCOUNT_SID or IMPACT_AUTH_TOKEN env vars")

AUTH = HTTPBasicAuth(ACCOUNT_SID, AUTH_TOKEN)

app = Flask(__name__)

def get_google_creds():
    """Load Google creds from base64 env var or local file."""
    b64 = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if b64:
        creds_json = json.loads(base64.b64decode(b64))
        return service_account.Credentials.from_service_account_info(
            creds_json,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
    elif os.path.exists("credentials.json"):
        return service_account.Credentials.from_service_account_file(
            "credentials.json",
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
    else:
        raise Exception("No Google credentials found. Set GOOGLE_CREDENTIALS_JSON env var or provide credentials.json")

def get_sheets_service():
    return build("sheets", "v4", credentials=get_google_creds())

# ==========================================================
# DATE HELPERS
# ==========================================================
def parse_iso_date(iso_str):
    try:
        if "T" in iso_str:
            return iso_str.split("T")[0]
        return iso_str
    except:
        return iso_str

def get_last_synced_date(service):
    """Read Sheet1 column A to find the most recent date already synced."""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_MAIN_AGGREGATE}'!A:A"
        ).execute()
        values = result.get("values", [])
        
        if len(values) <= 1:  # Only header or empty
            return None
        
        # Find the latest date (column A has dates, row 0 is header)
        latest = None
        for row in values[1:]:
            if row and row[0]:
                try:
                    d = datetime.strptime(row[0], "%Y-%m-%d").date()
                    if latest is None or d > latest:
                        latest = d
                except:
                    pass
        return latest
    except Exception as e:
        print(f"   ⚠️ Could not read last date: {e}")
        return None

def get_date_chunks(start_date, end_date):
    """Generate date chunks from start to end."""
    chunks = []
    current_start = start_date
    while current_start < end_date:
        current_end = current_start + timedelta(days=CHUNK_DAYS)
        if current_end > end_date:
            current_end = end_date
        chunks.append((
            f"{current_start.strftime('%Y-%m-%d')}T00:00:00-08:00",
            f"{current_end.strftime('%Y-%m-%d')}T00:00:00-08:00"
        ))
        current_start = current_end + timedelta(days=1)
    return chunks

# ==========================================================
# FETCH CAMPAIGNS
# ==========================================================
def fetch_campaigns():
    print("🔎 Fetching Campaign List...")
    url = f"{IMPACT_BASE_URL}/Advertisers/{ACCOUNT_SID}/Campaigns"
    
    for attempt in range(5):
        try:
            response = requests.get(url, auth=AUTH, headers={"Accept": "application/json"})
            
            if response.status_code == 200:
                data = response.json()
                campaigns = data.get("Campaigns", []) or data.get("Records", [])
                print(f"✅ Found {len(campaigns)} Campaigns.")
                return campaigns
            
            elif response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait = int(retry_after) if retry_after else 60
                if wait > 300:
                    wait = 300
                print(f"   ⏳ Rate limited. Waiting {wait}s (attempt {attempt+1}/5)...")
                time.sleep(wait)
                continue
            
            else:
                print(f"❌ Failed to list campaigns: {response.status_code}")
                return []
        except Exception as e:
            print(f"❌ Exception: {e}")
            return []
    
    print("❌ Rate limit not clearing.")
    return []

# ==========================================================
# FETCH ACTIONS (Conversions) — Chunked
# ==========================================================
def fetch_actions(campaigns, date_chunks):
    print(f"\n📊 Fetching Actions in {len(date_chunks)} chunks...")
    
    actions_data = defaultdict(lambda: {'actions': 0, 'revenue': 0.0, 'cost': 0.0})

    for campaign in campaigns:
        campaign_id = campaign.get("Id")
        campaign_name = campaign.get("Name")
        print(f"\n   🔄 Actions: {campaign_name} ({campaign_id})")

        for chunk_start, chunk_end in date_chunks:
            page = 1
            while True:
                params = {
                    "CampaignId": campaign_id,
                    "ActionDateStart": chunk_start,
                    "ActionDateEnd": chunk_end,
                    "Page": page,
                    "PageSize": PAGE_SIZE,
                    "Format": "JSON"
                }
                try:
                    response = requests.get(
                        f"{IMPACT_BASE_URL}/Advertisers/{ACCOUNT_SID}/Actions",
                        params=params, auth=AUTH,
                        headers={"Accept": "application/json"}
                    )
                    
                    if response.status_code == 429:
                        retry_after = response.headers.get("Retry-After")
                        wait = int(retry_after) if retry_after else 60
                        if wait > 300:
                            wait = 300
                        print(f"      ⏳ Rate limited. Waiting {wait}s...")
                        time.sleep(wait)
                        continue
                    
                    if response.status_code != 200:
                        break

                    data = response.json()
                    actions = data.get("Actions", []) or data.get("Records", [])
                    if not actions:
                        break

                    for action in actions:
                        raw_date = action.get("EventDate", "")
                        date_str = parse_iso_date(raw_date)
                        partner_name = action.get("MediaPartnerName", "Unknown")
                        revenue = float(action.get("Amount", 0.0))
                        payout = float(action.get("Payout", 0.0))
                        
                        key = (date_str, campaign_name, partner_name)
                        actions_data[key]['actions'] += 1
                        actions_data[key]['revenue'] += revenue
                        actions_data[key]['cost'] += payout

                    next_uri = data.get("@nextpageuri")
                    if not next_uri or len(actions) < PAGE_SIZE:
                        break
                    page += 1
                    time.sleep(RATE_LIMIT_DELAY)
                except Exception as e:
                    print(f"      ❌ Error: {e}")
                    break

    return actions_data

# ==========================================================
# FETCH CLICKS (via Async ClickExport)
# ==========================================================
def submit_click_job(campaign_id, start_date, end_date):
    url = f"{IMPACT_BASE_URL}/Advertisers/{ACCOUNT_SID}/Programs/{campaign_id}/ClickExport"
    params = {"DateStart": start_date, "DateEnd": end_date}
    
    for retry in range(CLICK_429_MAX_RETRIES):
        resp = requests.get(url, params=params, auth=AUTH, headers={"Accept": "application/json"})
        
        if resp.status_code == 200:
            data = resp.json()
            queued_uri = data.get("QueuedUri", "")
            if "/Jobs/" in queued_uri:
                return queued_uri.split("/Jobs/")[1].split("/")[0]
            return None
        
        elif resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            wait = int(retry_after) if retry_after else CLICK_429_BASE_WAIT * (retry + 1)
            if wait > CLICK_429_MAX_WAIT:
                print(f"      ⚠️ Retry-After {wait}s. Skipping (rate limited).")
                return "RATE_LIMITED"
            print(f"      ⏳ 429. Waiting {wait}s (retry {retry+1}/{CLICK_429_MAX_RETRIES})...")
            time.sleep(wait)
        else:
            print(f"      ⚠️ Submit failed: {resp.status_code}")
            return None
    
    return None

def fetch_clicks(campaigns, start_date, end_date):
    print(f"\n🖱️ Fetching Clicks via ClickExport...")
    
    clicks_data = defaultdict(lambda: {'clicks': 0, 'cpc_total': 0.0})
    
    for i, campaign in enumerate(campaigns):
        campaign_id = campaign.get("Id")
        campaign_name = campaign.get("Name")
        print(f"\n   🔄 Clicks [{i+1}/{len(campaigns)}]: {campaign_name} ({campaign_id})")
        
        if i > 0:
            print(f"      ⏳ Waiting {CLICK_BETWEEN_CAMPAIGNS}s...")
            time.sleep(CLICK_BETWEEN_CAMPAIGNS)
        
        try:
            job_id = submit_click_job(campaign_id, start_date, end_date)
            if job_id == "RATE_LIMITED":
                print(f"\n   ⚠️ ClickExport rate limited. Skipping remaining.")
                break
            if not job_id:
                continue
            
            print(f"      📋 Job: {job_id}")
            
            job_url = f"{IMPACT_BASE_URL}/Advertisers/{ACCOUNT_SID}/Jobs/{job_id}"
            completed = False
            
            for _ in range(CLICK_MAX_POLLS):
                time.sleep(CLICK_POLL_INTERVAL)
                resp = requests.get(job_url, auth=AUTH, headers={"Accept": "application/json"})
                if resp.status_code == 200:
                    status = resp.json().get("Status", "UNKNOWN")
                    if status == "COMPLETED":
                        completed = True
                        break
                    elif status in ["FAILED", "CANCELLED"]:
                        break
            
            if not completed:
                print(f"      ⚠️ Job did not complete")
                continue
            
            download_url = f"{IMPACT_BASE_URL}/Advertisers/{ACCOUNT_SID}/Jobs/{job_id}/Download"
            resp = requests.get(download_url, auth=AUTH)
            
            if resp.status_code == 200:
                reader = csv.DictReader(io.StringIO(resp.text))
                rows = list(reader)
                print(f"      ✅ {len(rows)} clicks")
                
                for row in rows:
                    event_date = row.get("EventDate", "")
                    date_str = parse_iso_date(event_date)
                    partner_name = row.get("MediaName", "Unknown")
                    
                    key = (date_str, campaign_name, partner_name)
                    clicks_data[key]['clicks'] += 1
            
        except Exception as e:
            print(f"      ❌ Error: {e}")
            continue

    return clicks_data

# ==========================================================
# MERGE DATA INTO ROWS
# ==========================================================
def merge_data(actions_data, clicks_data):
    print(f"\n✅ Merging {len(actions_data)} action keys + {len(clicks_data)} click keys...")
    
    all_keys = set(actions_data.keys()) | set(clicks_data.keys())
    rows = []
    brand_rows = defaultdict(list)
    
    sorted_keys = sorted(all_keys, key=lambda x: x[0], reverse=True)
    
    for key in sorted_keys:
        date_str, campaign_name, partner_name = key
        
        a = actions_data.get(key, {'actions': 0, 'revenue': 0.0, 'cost': 0.0})
        c = clicks_data.get(key, {'clicks': 0, 'cpc_total': 0.0})
        
        actions = a['actions']
        revenue = round(a['revenue'], 2)
        total_cost = round(a['cost'], 2)
        clicks = c['clicks']
        cpc = round(total_cost / clicks, 2) if clicks > 0 else 0.0
        
        row = [date_str, campaign_name, partner_name, clicks, actions, revenue, total_cost, total_cost, cpc]
        rows.append(row)
        
        clean_name = campaign_name.replace(":", "").replace("/", "-")[:30]
        brand_rows[clean_name].append(row)

    return rows, brand_rows

# ==========================================================
# GOOGLE SHEETS — INCREMENTAL APPEND
# ==========================================================
def ensure_sheet_exists(service, sheet_title):
    try:
        metadata = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        existing = [s['properties']['title'] for s in metadata.get('sheets', [])]
        
        if sheet_title not in existing:
            print(f"      🆕 Creating: {sheet_title}")
            service.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={'requests': [{'addSheet': {'properties': {'title': sheet_title}}}]}
            ).execute()
            return True  # New sheet, needs header
        return False
    except Exception as e:
        print(f"      ⚠️ Error: {e}")
        return False

def sheet_has_data(service, sheet_title):
    """Check if sheet has any data."""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{sheet_title}'!A1"
        ).execute()
        return bool(result.get("values"))
    except:
        return False

def clear_all_sheets(service):
    """Delete all sheets except Sheet1, then clear Sheet1."""
    print("\n🧹 Clearing ALL existing sheets...")
    try:
        metadata = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        sheets = metadata.get('sheets', [])
        
        delete_requests = []
        for sheet in sheets:
            title = sheet['properties']['title']
            sheet_id = sheet['properties']['sheetId']
            if title != SHEET_MAIN_AGGREGATE:
                print(f"   🗑️ Deleting: {title}")
                delete_requests.append({'deleteSheet': {'sheetId': sheet_id}})
        
        if delete_requests:
            service.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={'requests': delete_requests}
            ).execute()
        
        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_MAIN_AGGREGATE}'!A:Z"
        ).execute()
        print("   ✅ Cleared.")
    except Exception as e:
        print(f"   ⚠️ Error: {e}")

def sync_to_sheets(master_rows, brand_rows, full_refresh):
    service = get_sheets_service()
    sheets = service.spreadsheets()
    
    header = [["DATE", "CAMPAIGN", "PARTNER", "CLICKS", "ACTIONS", "REVENUE USD", "ACTIONS COST USD", "TOTAL COST USD", "CPC USD"]]

    if full_refresh:
        clear_all_sheets(service)
    
    print("\n📤 Syncing to Google Sheets...")

    # --- Sheet1 (Master) ---
    print(f"   👉 '{SHEET_MAIN_AGGREGATE}': {len(master_rows)} rows")
    
    if full_refresh or not sheet_has_data(service, SHEET_MAIN_AGGREGATE):
        # Write header + data
        sheets.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_MAIN_AGGREGATE}'!A1",
            valueInputOption="RAW",
            body={"values": header + master_rows}
        ).execute()
    else:
        # Append new rows
        sheets.values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_MAIN_AGGREGATE}'!A:I",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": master_rows}
        ).execute()
    
    # --- Brand sheets ---
    for sheet_name, data_rows in brand_rows.items():
        print(f"   👉 '{sheet_name}': {len(data_rows)} rows")
        
        is_new = ensure_sheet_exists(service, sheet_name)
        
        if full_refresh or is_new:
            sheets.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{sheet_name}'!A1",
                valueInputOption="RAW",
                body={"values": header + data_rows}
            ).execute()
        else:
            sheets.values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{sheet_name}'!A:I",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": data_rows}
            ).execute()

# ==========================================================
# MAIN
# ==========================================================
def main():
    full_refresh = "--full" in sys.argv
    skip_clicks = "--skip-clicks" in sys.argv
    
    print("🚀 Impact Data Sync")
    print(f"   Mode: {'FULL REFRESH' if full_refresh else 'INCREMENTAL'}")
    
    # Determine date range
    service = get_sheets_service()
    
    if full_refresh:
        start_date = (datetime.today() - timedelta(days=1095)).date()  # 3 years
        print(f"   Full refresh from {start_date}")
    else:
        last_date = get_last_synced_date(service)
        if last_date:
            start_date = last_date  # Re-fetch last date to capture any late actions
            print(f"   Last synced: {last_date} → fetching from {start_date}")
        else:
            start_date = (datetime.today() - timedelta(days=1095)).date()
            full_refresh = True  # Empty sheet, do full refresh
            print(f"   No data found. Full refresh from {start_date}")
    
    end_date = datetime.today().date()
    
    if start_date >= end_date:
        print("✅ Already up to date. Nothing to sync.")
        return
    
    # Fetch campaigns
    campaigns = fetch_campaigns()
    if not campaigns:
        print("❌ No campaigns found.")
        return
    
    # Fetch Actions
    date_chunks = get_date_chunks(start_date, end_date)
    actions_data = fetch_actions(campaigns, date_chunks)
    
    # Fetch Clicks
    if skip_clicks:
        print("\n   ⏭️ Clicks: SKIPPED")
        clicks_data = {}
    else:
        clicks_data = fetch_clicks(campaigns, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
    
    # Merge
    master_rows, brand_rows = merge_data(actions_data, clicks_data)
    
    if not master_rows:
        print("✅ No new data found.")
        return
    
    # Sync to Sheets
    sync_to_sheets(master_rows, brand_rows, full_refresh)
    
    print(f"\n🎉 SYNC COMPLETE — {len(master_rows)} rows synced.")

def run_sync():
    main()


if __name__ == "__main__":
    # Start your sync logic in a background thread
    threading.Thread(target=run_sync).start()

    # Bind to Render-required host + port
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

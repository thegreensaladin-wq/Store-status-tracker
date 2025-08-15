import os, re, time, atexit, gc
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Any, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed

import gspread
from google.oauth2.service_account import Credentials

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.support import expected_conditions as EC

# ==================== ENV / CONFIG ====================
load_dotenv()

SHEET_ID   = os.getenv("SHEET_ID", "").strip()
INPUT_TABS = [t.strip() for t in os.getenv("INPUT_TABS", "").split(",") if t.strip()]
SA_JSON    = os.getenv("GCP_SA_JSON_PATH", "service_account.json").strip()
HEADLESS   = os.getenv("HEADLESS", "true").lower() == "true"
TZ_NAME    = os.getenv("TIMEZONE", "Asia/Kolkata").strip()
UC_VERSION = os.getenv("UC_VERSION_MAIN", "").strip()

PAGELOAD_TIMEOUT = int(os.getenv("PAGELOAD_TIMEOUT", "45"))
AFTER_LOAD_WAIT  = int(os.getenv("AFTER_LOAD_WAIT", "10"))
BETWEEN_STORES_S = float(os.getenv("BETWEEN_STORES_SECONDS", "1"))
MAX_WORKERS      = max(1, int(os.getenv("MAX_WORKERS", "5")))

# ==================== TIMEZONE ====================
try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    def get_tz():
        try:
            return ZoneInfo(TZ_NAME)
        except ZoneInfoNotFoundError:
            if TZ_NAME in ("Asia/Kolkata", "Asia/Calcutta"):
                return timezone(timedelta(hours=5, minutes=30))
            return datetime.now().astimezone().tzinfo or timezone.utc
except Exception:
    def get_tz():
        return timezone(timedelta(hours=5, minutes=30))

def now_date_str(): return datetime.now(get_tz()).strftime("%Y-%m-%d")
def now_time_str(): return datetime.now(get_tz()).strftime("%H:%M:%S")

# ==================== SHEETS HELPERS ====================
def open_client_and_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SA_JSON, scopes=scopes)
    gc_ = gspread.authorize(creds)
    sh = gc_.open_by_key(SHEET_ID)
    return gc_, sh

def get_or_all_tabs(sh):
    return INPUT_TABS if INPUT_TABS else [ws.title for ws in sh.worksheets()]

def find_header_row_and_columns(all_values: List[List[str]]):
    """Find row with headers: Brand, Location, Aggregator, Link, Latitude, Longitude (case-insensitive)."""
    wanted = ["brand", "location", "aggregator", "link", "latitude", "longitude"]
    for i, row in enumerate(all_values, start=1):
        lowers = [c.strip().lower() for c in row]
        if all(w in lowers for w in wanted):
            idx = {name: lowers.index(name) + 1 for name in wanted}  # 1-based
            return i, idx
    raise SystemExit("Header row not found. Expected columns: Brand, Location, Aggregator, Link, Latitude, Longitude")

def first_empty_log_column(ws, start_col: int) -> int:
    """Find next empty column by checking row 1 and row 2 (both blank means free)."""
    row1 = ws.row_values(1)
    row2 = ws.row_values(2)
    def is_empty(col):
        v1 = row1[col-1] if col-1 < len(row1) else ""
        v2 = row2[col-1] if col-1 < len(row2) else ""
        return (v1.strip() == "") and (v2.strip() == "")
    col = start_col
    while True:
        if is_empty(col):
            return col
        col += 1

# ==================== BROWSER / SCRAPING ====================
def make_driver():
    opts = uc.ChromeOptions()
    prefs = {
        "profile.default_content_setting_values.geolocation": 1,
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False
    }
    opts.add_experimental_option("prefs", prefs)
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--start-maximized")
    if HEADLESS:
        opts.add_argument("--headless=new")

    kw = {"options": opts}
    if UC_VERSION.isdigit():
        kw["version_main"] = int(UC_VERSION)

    driver = uc.Chrome(**kw)
    driver.set_page_load_timeout(PAGELOAD_TIMEOUT)

    def _safe_quit():
        try: driver.quit()
        except Exception: pass
        try: gc.collect()
        except Exception: pass
    atexit.register(_safe_quit)
    return driver

def origin_of(url: str) -> str:
    if url.startswith("http"):
        parts = url.split("/")
        return f"{parts[0]}//{parts[2]}"
    return "https://www.swiggy.com"

def grant_geo(driver, url: str, lat: float=None, lng: float=None):
    if lat is None or lng is None:
        return
    try:
        driver.execute_cdp_cmd("Browser.grantPermissions", {"origin": origin_of(url), "permissions": ["geolocation"]})
        driver.execute_cdp_cmd("Emulation.setGeolocationOverride", {"latitude": float(lat), "longitude": float(lng), "accuracy": 50})
    except Exception:
        pass

ETA_PAT = re.compile(r"\b(\d+)\s*(?:–|-|to)?\s*(\d+)?\s*mins?\b", re.I)

def parse_eta_from_text(texts: List[str]) -> str:
    hits = []
    for t in texts:
        for m in ETA_PAT.finditer(t):
            hits.append(m.group(0).strip())
    if not hits:
        joined = " | ".join(texts)
        for m in ETA_PAT.finditer(joined):
            hits.append(m.group(0).strip())
    if not hits:
        return ""
    hits.sort(key=len)
    return hits[0]

def extract_texts(driver, locs: List[Tuple[str,str]], max_elems=60) -> List[str]:
    out, seen = [], set()
    for by, value in locs:
        b = By.XPATH if by == "xpath" else By.CSS_SELECTOR
        try:
            elems = driver.find_elements(b, value)
            for e in elems[:max_elems]:
                t = e.text.strip()
                if t and t not in seen:
                    out.append(t); seen.add(t)
        except WebDriverException:
            continue
    return out

def infer_status(texts: List[str]) -> str:
    j = " | ".join(t.lower() for t in texts)
    if "temporarily closed" in j or "closed" in j:
        return "Closed"
    if "not accepting" in j or "currently not accepting" in j:
        return "Not accepting orders"
    m = re.search(r"opens?\s+at\s+([0-9:\sapm\.]+)", j)
    if m:
        return f"Opens at {m.group(1).strip()}"
    return "Available"

# Swiggy locators
SW_STATUS_LOCS = [
    ("xpath", "//*[contains(translate(., 'CLOSED', 'closed'), 'closed')]"),
    ("xpath", "//*[contains(translate(., 'NOT ACCEPTING', 'not accepting'), 'not accepting')]"),
    ("xpath", "//*[contains(translate(., 'OPENS AT', 'opens at'), 'opens at')]"),
    ("xpath", "//*[contains(., 'Currently unavailable')]"),
    ("xpath", "//*[contains(., 'Unavailable in your area')]"),
    ("css",   "[class*='status'], [class*='badge'], [class*='banner']"),
]
SW_ETA_LOCS = [
    ("xpath", "//*[contains(translate(., 'MINS', 'mins'), 'mins')]//span"),
    ("xpath", "//span[contains(translate(., 'MINS', 'mins'), 'mins')]"),
    ("css",   "[class*='minute'], [class*='mins'], [class*='eta'], [class*='delivery']"),
]
SW_SOLDOUT_LOCS = [
    ("xpath", "//*[contains(translate(., 'SOLD OUT', 'sold out'), 'sold out')]"),
    ("xpath", "//*[contains(translate(., 'UNAVAILABLE', 'unavailable'), 'unavailable')]"),
]

# Zomato locators
ZO_STATUS_LOCS = [
    ("xpath", "//*[contains(translate(., 'CLOSED', 'closed'), 'closed')]"),
    ("xpath", "//*[contains(translate(., 'OPENS AT', 'opens at'), 'opens at')]"),
    ("xpath", "//*[contains(translate(., 'NOT ACCEPTING', 'not accepting'), 'not accepting')]"),
    ("xpath", "//*[contains(., 'Temporarily closed')]"),
    ("xpath", "//*[contains(., 'Currently not accepting orders')]"),
]
ZO_ETA_LOCS = [
    ("xpath", "//*[contains(translate(., 'MINS', 'mins'), 'mins')]"),
    ("css",   "[class*='minute'], [class*='mins'], [class*='time'], [class*='eta']"),
]

@retry(stop=stop_after_attempt(2), wait=wait_fixed(2))
def scrape_store(aggregator: str, url: str, lat: float=None, lng: float=None) -> str:
    """Create a short-lived driver per store (safe for parallelism). Return compact status string."""
    if not url.startswith("http"):
        url = "https://" + url.lstrip("/")

    driver = make_driver()
    try:
        grant_geo(driver, url, lat, lng)
        driver.get(url)
        time.sleep(AFTER_LOAD_WAIT)
        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        except TimeoutException:
            pass

        if aggregator.lower().startswith("swiggy"):
            status_texts = extract_texts(driver, SW_STATUS_LOCS)
            eta_texts    = extract_texts(driver, SW_ETA_LOCS)
            sold_out     = len(extract_texts(driver, SW_SOLDOUT_LOCS, max_elems=300))
            status       = infer_status(status_texts)
            eta          = parse_eta_from_text(eta_texts)
            compact      = status if not eta else f"{status} | {eta}"
            if sold_out:
                compact += f" | SO:{sold_out}"
            return compact

        # Zomato
        status_texts = extract_texts(driver, ZO_STATUS_LOCS)
        eta_texts    = extract_texts(driver, ZO_ETA_LOCS)
        status       = infer_status(status_texts)
        eta          = parse_eta_from_text(eta_texts)
        return status if not eta else f"{status} | {eta}"

    finally:
        try: driver.quit()
        except Exception: pass

def to_float(x: Any):
    try:
        s = str(x).strip()
        return float(s) if s else None
    except Exception:
        return None

# ==================== PER-TAB PROCESSING ====================
def process_one_tab(sh, tab_name: str) -> None:
    ws = sh.worksheet(tab_name)
    all_vals = ws.get_all_values()
    if not all_vals:
        print(f"[{tab_name}] Empty sheet; skipping.")
        return

    header_row, cols = find_header_row_and_columns(all_vals)
    col_brand    = cols["brand"]
    col_location = cols["location"]
    col_agg      = cols["aggregator"]
    col_link     = cols["link"]
    col_lat      = cols["latitude"]
    col_lng      = cols["longitude"]

    # Our horizontal log begins at the first column right of Longitude
    start_log_col = col_lng + 1
    log_col = first_empty_log_column(ws, start_log_col)

    # Stamp Date (row 1) + Time (row 2)
    ws.update_cell(1, log_col, now_date_str())
    ws.update_cell(2, log_col, now_time_str())

    # Build jobs from row 3 onwards (as you requested)
    jobs = []
    for r in range(max(3, header_row + 1), len(all_vals) + 1):
        row_vals = all_vals[r-1]
        def getv(ci): return row_vals[ci-1] if ci-1 < len(row_vals) else ""
        agg   = getv(col_agg).strip()
        link  = getv(col_link).strip()
        lat   = to_float(getv(col_lat))
        lng   = to_float(getv(col_lng))
        if not agg or not link:
            jobs.append((r, "Missing link/aggregator"))
        else:
            jobs.append((r, (agg, link, lat, lng)))

    # Run scraping in parallel for this tab
    results: Dict[int, str] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        future_map = {}
        for r, payload in jobs:
            if isinstance(payload, tuple):
                agg, link, lat, lng = payload
                fut = pool.submit(scrape_store, agg, link, lat, lng)
                future_map[fut] = r
            else:
                # Already has a message (missing data)
                results[r] = payload

        for fut in as_completed(future_map):
            r = future_map[fut]
            try:
                compact = fut.result()
            except Exception as e:
                compact = f"Error: {type(e).__name__}"
            results[r] = compact
            time.sleep(BETWEEN_STORES_S)

    # Batch update this tab’s column
    updates = []
    for r, v in sorted(results.items()):
        a1 = gspread.utils.rowcol_to_a1(r, log_col)
        updates.append({"range": a1, "values": [[v]]})
    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")

    print(f"[{tab_name}] Logged {len(results)} rows to column {log_col}.")

# ==================== MAIN ====================
def main():
    if not SHEET_ID:
        raise SystemExit("Set SHEET_ID in .env")
    gc_, sh = open_client_and_sheet()
    tabs = get_or_all_tabs(sh)
    print("Tabs to process:", tabs)

    # Process tabs sequentially, but each tab scrapes its rows in parallel.
    # (If you want FULL parallel tabs too, you can wrap this loop with another ThreadPoolExecutor.)
    for tab in tabs:
        try:
            process_one_tab(sh, tab)
        except Exception as e:
            print(f"[{tab}] Failed: {type(e).__name__}: {e}")

if __name__ == "__main__":
    main()
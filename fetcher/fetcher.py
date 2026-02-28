"""
Data fetcher – checks for new folders, downloads ALL .7z archives,
extracts the NDJSON .txt, imports into PostgreSQL.

FLEXIBLE: Works with any folder naming (not just 'day*') and handles
multiple .7z files per folder. Tracks by file URL hash to prevent duplicates.

In MANUAL_MODE: runs a gunicorn API server for on-demand fetches.
Otherwise: runs once on startup (bootstrap if DB empty) then every CHECK_INTERVAL.
"""

import os
import re
import sys
import time
import random
import shutil
import hashlib
import subprocess
import threading
from datetime import datetime, timezone

import requests
import psycopg2
import psycopg2.sql as sql
import orjson
from flask import Flask, jsonify

# ── Config ──────────────────────────────────────────────────────
sys.stdout.reconfigure(line_buffering=True)

SOURCE_URL = os.getenv("SOURCE_URL", "http://37.72.140.17/pay_or_leak/odido/")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "21600"))  # 6 hours
MANUAL_MODE = os.getenv("MANUAL_MODE", "0").lower() in ("1", "true", "yes")
FETCHER_API_PORT = int(os.getenv("FETCHER_API_PORT", "8000"))

DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
WEBAPP_USER = os.environ["WEBAPP_USER"]
WEBAPP_PASSWORD = os.environ["WEBAPP_PASSWORD"]

ID_FIELD = "Id"
BATCH_SIZE = 50000
WORK_DIR = "/tmp/fetcher"
DOWNLOAD_RETRIES = int(os.getenv("DOWNLOAD_RETRIES", "3"))

SEARCH_INDEX_COLUMNS = [
    "Name",
    "vlocity_cmt__BillingEmailAddress__c",
    "Phone",
    "BillingCity",
    "BillingPostalCode",
    "BillingStreet",
    "House_Number__c",
    "Bank_Account_Number__c",
    "Bank_Account_Holder_Name__c",
    "Id",
    "Account_Salesforce_ID__c",
    "ParentAccountName__c",
    "Description",
    "Flash_Message__c",
]

_fetch_lock = threading.Lock()
_fetch_state = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "progress": "Idle",
    "error": None,
    "files_imported": 0,
    "total_rows": 0,
    "log": [],
}


# ── Logging ─────────────────────────────────────────────────────
def log(msg: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with _fetch_lock:
        _fetch_state["log"].append(line)
        if len(_fetch_state["log"]) > 200:
            _fetch_state["log"] = _fetch_state["log"][-200:]


# ── Database helpers ────────────────────────────────────────────
def connect():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
    )


def get_imported_days(conn):
    """Return set of day numbers already imported (based on tracking table)."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS _import_tracker (
            day_num   INTEGER PRIMARY KEY,
            day_name  TEXT,
            table_name TEXT,
            row_count INTEGER,
            imported_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.execute("SELECT day_num FROM _import_tracker ORDER BY day_num;")
    days = {row[0] for row in cur.fetchall()}
    cur.close()
    return days


def get_imported_files(conn):
    """Return set of file URLs/hashes already imported (nieuwe tracking tabel)."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS _file_tracker (
            file_hash TEXT PRIMARY KEY,
            file_url TEXT,
            folder_name TEXT,
            table_name TEXT,
            row_count INTEGER,
            imported_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.execute("SELECT file_hash FROM _file_tracker;")
    files = {row[0] for row in cur.fetchall()}
    cur.close()
    return files


def record_import(conn, day_num, day_name, table_name, row_count):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO _import_tracker (day_num, day_name, table_name, row_count) "
        "VALUES (%s, %s, %s, %s) ON CONFLICT (day_num) DO NOTHING;",
        (day_num, day_name, table_name, row_count),
    )
    conn.commit()
    cur.close()


def record_file_import(conn, file_url, folder_name, table_name, row_count):
    """Track imported file by URL hash to prevent re-importing."""
    cur = conn.cursor()
    file_hash = hashlib.sha256(file_url.encode()).hexdigest()
    cur.execute(
        "INSERT INTO _file_tracker (file_hash, file_url, folder_name, table_name, row_count) "
        "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (file_hash) DO NOTHING;",
        (file_hash, file_url, folder_name, table_name, row_count),
    )
    conn.commit()
    cur.close()


def grant_select(conn, table_name):
    """Give the read-only webapp user SELECT on the new table."""
    cur = conn.cursor()
    try:
        cur.execute(
            sql.SQL("GRANT SELECT ON {} TO {}").format(
                sql.Identifier(table_name),
                sql.Identifier(WEBAPP_USER),
            )
        )
        conn.commit()
        log(f"  Granted SELECT on {table_name} to {WEBAPP_USER}")
    except Exception as e:
        conn.rollback()
        log(f"  Warning: could not grant SELECT to {WEBAPP_USER}: {e}")
    cur.close()


def grant_select_tracker(conn):
    """Grant SELECT on _import_tracker and _file_tracker to webapp user."""
    cur = conn.cursor()
    try:
        cur.execute(
            sql.SQL("GRANT SELECT ON _import_tracker TO {}").format(
                sql.Identifier(WEBAPP_USER)
            )
        )
        cur.execute(
            sql.SQL("GRANT SELECT ON _file_tracker TO {}").format(
                sql.Identifier(WEBAPP_USER)
            )
        )
        conn.commit()
    except Exception:
        conn.rollback()
    cur.close()


def db_has_data(conn):
    """Check if there are any imported tables at all."""
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM _import_tracker;")
        count = cur.fetchone()[0]
        cur.close()
        return count > 0
    except Exception:
        conn.rollback()
        cur.close()
        return False


def ensure_webapp_user(conn):
    """Create the read-only webapp user if it doesn't exist."""
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s;", (WEBAPP_USER,))
        if not cur.fetchone():
            cur.execute(
                sql.SQL("CREATE USER {} WITH PASSWORD %s").format(
                    sql.Identifier(WEBAPP_USER)
                ),
                (WEBAPP_PASSWORD,),
            )
            log(f"Created database user '{WEBAPP_USER}'")
        cur.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(DB_NAME),
                sql.Identifier(WEBAPP_USER),
            )
        )
        cur.execute(
            sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(
                sql.Identifier(WEBAPP_USER)
            )
        )
        cur.execute(
            sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA information_schema TO {}").format(
                sql.Identifier(WEBAPP_USER)
            )
        )
        cur.execute(
            sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA public TO {}").format(
                sql.Identifier(WEBAPP_USER)
            )
        )
        cur.execute(
            sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {}").format(
                sql.Identifier(WEBAPP_USER)
            )
        )
        conn.commit()
        log(f"Ensured '{WEBAPP_USER}' has base permissions")
    except Exception as e:
        conn.rollback()
        log(f"Warning: could not ensure webapp user: {e}")
    cur.close()


def _idx_name(table_name, col_name, kind):
    digest = hashlib.md5(f"{table_name}:{col_name}:{kind}".encode()).hexdigest()[:10]
    return f"idx_{table_name}_{kind}_{digest}"


def ensure_search_indexes(conn, table_name):
    """Create fast search indexes for allowed search columns."""
    cur = conn.cursor()
    try:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s",
            (table_name,),
        )
        existing = {row[0] for row in cur.fetchall()}

        for col in SEARCH_INDEX_COLUMNS:
            if col not in existing:
                continue

            trigram_idx = _idx_name(table_name, col, "trgm")
            prefix_idx = _idx_name(table_name, col, "lower")

            cur.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} USING GIN ({} gin_trgm_ops)").format(
                    sql.Identifier(trigram_idx),
                    sql.Identifier(table_name),
                    sql.Identifier(col),
                )
            )
            cur.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ((LOWER({})))").format(
                    sql.Identifier(prefix_idx),
                    sql.Identifier(table_name),
                    sql.Identifier(col),
                )
            )

        conn.commit()
        log(f"  Search indexes ensured for {table_name}")
    except Exception as e:
        conn.rollback()
        log(f"  Warning: could not build search indexes on {table_name}: {e}")
    finally:
        cur.close()


# ── Import logic (from import_ndjson.py) ────────────────────────
def infer_keys(path: str):
    with open(path, "rb") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = orjson.loads(line)
            if isinstance(obj, dict):
                return list(obj.keys())
    raise ValueError(f"No JSON objects in {path}")


def get_existing_columns(cur, table):
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = %s AND table_schema = 'public'",
        (table,),
    )
    return {row[0] for row in cur.fetchall()}


def ensure_columns(cur, table, keys, existing):
    missing = [k for k in keys if k not in existing]
    for k in missing:
        cur.execute(sql.SQL("ALTER TABLE {} ADD COLUMN {} TEXT").format(
            sql.Identifier(table), sql.Identifier(k),
        ))
    return existing | set(missing)


def _flush_batch(cur, table, cols, batch):
    if not batch:
        return
    col_sql = sql.SQL(", ").join(sql.Identifier(c) for c in cols)
    has_id = "Id" in cols
    if has_id:
        query = sql.SQL("INSERT INTO {} ({}) VALUES %s ON CONFLICT ({}) DO NOTHING").format(
            sql.Identifier(table), col_sql, sql.Identifier("Id"),
        )
    else:
        query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.Identifier(table), col_sql,
        )
    execute_values(cur, query.as_string(cur), batch, page_size=len(batch))


def import_file(conn, table_name, file_path):
    """Import an NDJSON .txt file into the given table. Returns row count."""
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    log(f"  Importing {os.path.basename(file_path)} ({file_size_mb:.1f} MB) -> {table_name}")
    t0 = time.time()

    known_keys = set(infer_keys(file_path))
    cur = conn.cursor()

    # Create table
    cols_sql = sql.SQL(", ").join(
        sql.SQL("{} TEXT").format(sql.Identifier(k)) for k in known_keys
    )
    cur.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
        sql.Identifier(table_name), cols_sql,
    ))
    # Ensure unique constraint on Id to prevent duplicates
    idx_name = f"idx_{table_name}_id_uniq"
    cur.execute(sql.SQL("CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({})").format(
        sql.Identifier(idx_name), sql.Identifier(table_name), sql.Identifier("Id"),
    ))
    conn.commit()

    # Disable synchronous_commit for this session — safe for bulk loads
    cur.execute("SET synchronous_commit = off;")

    known_keys = get_existing_columns(cur, table_name)
    inserted = 0
    batch_cols = None
    batch_values = []
    # Pre-compute sorted cols tuple once; only recompute when schema changes
    current_sorted_cols = tuple(sorted(known_keys))

    with open(file_path, "rb") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            try:
                obj = orjson.loads(raw)
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue

            keys = list(obj.keys())
            new_keys = set(keys) - known_keys
            if new_keys:
                if batch_values:
                    _flush_batch(cur, table_name, batch_cols, batch_values)
                    batch_values = []
                known_keys = ensure_columns(cur, table_name, list(new_keys), known_keys)
                conn.commit()
                current_sorted_cols = tuple(sorted(known_keys))

            cols = current_sorted_cols
            values = tuple(str(obj.get(c, "")) if obj.get(c) is not None else None for c in cols)

            if batch_cols is None:
                batch_cols = cols
            elif batch_cols != cols:
                _flush_batch(cur, table_name, batch_cols, batch_values)
                batch_values = []
                batch_cols = cols

            batch_values.append(values)
            inserted += 1

            if len(batch_values) >= BATCH_SIZE:
                _flush_batch(cur, table_name, batch_cols, batch_values)
                batch_values = []
                conn.commit()
                if inserted % 50000 == 0:
                    elapsed = time.time() - t0
                    rate = inserted / elapsed if elapsed > 0 else 0
                    log(f"    {table_name}: {inserted:,} rows ({rate:,.0f}/sec)")

    if batch_values:
        _flush_batch(cur, table_name, batch_cols, batch_values)
    conn.commit()

    # Create indexes
    if ID_FIELD in known_keys:
        cur.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
            sql.Identifier(f"idx_{table_name}_{ID_FIELD}"),
            sql.Identifier(table_name),
            sql.Identifier(ID_FIELD),
        ))
    if "Name" in known_keys:
        cur.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
            sql.Identifier(f"idx_{table_name}_Name"),
            sql.Identifier(table_name),
            sql.Identifier("Name"),
        ))
    conn.commit()
    cur.close()

    elapsed = time.time() - t0
    rate = inserted / elapsed if elapsed > 0 else 0
    log(f"  Done: {inserted:,} rows in {elapsed:.1f}s ({rate:,.0f}/sec)")
    return inserted


# ── Web scraping ────────────────────────────────────────────────
def discover_folders():
    """
    Scrape the index page and return list of (folder_name, folder_url).
    Flexible: vindt ALLE folders, niet alleen 'day*' pattern.
    """
    try:
        resp = requests.get(SOURCE_URL, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        log(f"Error fetching index: {e}")
        return []

    # Vind alle directory links (eindigen met /)
    # Negeer parent directory (..) en absolute paths
    pattern = re.compile(r'href="([^/"]+/)"', re.IGNORECASE)
    folders = []
    for match in pattern.finditer(resp.text):
        folder_path = match.group(1)
        # Skip parent directory en hidden folders
        folder_name = folder_path.rstrip("/")
        if folder_name in (".", "..", "") or folder_name.startswith("."):
            continue
        folder_url = SOURCE_URL.rstrip("/") + "/" + folder_path
        folders.append((folder_name, folder_url))

    log(f"Discovered {len(folders)} folder(s): {[f[0] for f in folders]}")
    return folders


def find_all_7z_in_folder(folder_url):
    """
    Find ALL .7z file URLs in a folder page.
    Returns list of (filename, full_url).
    """
    try:
        resp = requests.get(folder_url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        log(f"  Error fetching {folder_url}: {e}")
        return []

    pattern = re.compile(r'href="([^"]+\.7z)"', re.IGNORECASE)
    files = []
    for match in pattern.finditer(resp.text):
        filename = match.group(1)
        # Skip parent paths
        if "/" in filename or filename.startswith(".."):
            continue
        full_url = folder_url.rstrip("/") + "/" + filename
        files.append((filename, full_url))
    
    return files


def download_file(url, dest_path):
    """Download a file with progress logging."""
    log(f"  Downloading {url}")
    resp = requests.get(url, stream=True, timeout=600)
    resp.raise_for_status()
    total = int(resp.headers.get("content-length", 0))
    downloaded = 0
    last_log = 0

    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=4 * 1024 * 1024):
            f.write(chunk)
            downloaded += len(chunk)
            if total and downloaded - last_log > 50 * 1024 * 1024:
                pct = downloaded / total * 100
                log(f"    {downloaded / (1024*1024):.0f} MB / {total / (1024*1024):.0f} MB ({pct:.0f}%)")
                last_log = downloaded

    size_mb = os.path.getsize(dest_path) / (1024 * 1024)
    log(f"  Downloaded {size_mb:.1f} MB")


def extract_7z(archive_path, extract_dir):
    """Extract .7z using 7z command line tool. Returns path to first .txt found."""
    log(f"  Extracting {os.path.basename(archive_path)}")
    result = subprocess.run(
        ["7z", "x", "-y", f"-o{extract_dir}", archive_path],
        check=False, capture_output=True, text=True,
    )
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        detail = stderr if stderr else stdout
        detail = detail[-800:] if detail else "no output"
        raise RuntimeError(f"7z extract failed (exit={result.returncode}): {detail}")
    # Find the .txt file
    for root, dirs, files in os.walk(extract_dir):
        for f in files:
            if f.endswith(".txt") and not f.startswith("OPEN") and not f.startswith("This_Is"):
                return os.path.join(root, f)
    # Fallback: any .txt
    for root, dirs, files in os.walk(extract_dir):
        for f in files:
            if f.endswith(".txt"):
                return os.path.join(root, f)
    return None


def test_7z_archive(archive_path):
    """Return (ok, detail) after running a 7z integrity test."""
    result = subprocess.run(
        ["7z", "t", archive_path],
        check=False, capture_output=True, text=True,
    )
    if result.returncode == 0:
        return True, "ok"

    stderr = (result.stderr or "").strip()
    stdout = (result.stdout or "").strip()
    detail = stderr if stderr else stdout
    detail = detail[-800:] if detail else "no output"
    return False, f"7z test failed (exit={result.returncode}): {detail}"


# ── Main logic ──────────────────────────────────────────────────
def generate_table_name(folder_name, filename):
    """
    Generate a simple PostgreSQL table name from folder name.
    Example: folder='day1' -> 'data_day1'
    """
    # Extract folder name and lowercase
    safe_folder = folder_name.lower()
    # Prefix met 'data_' voor duidelijkheid
    return f"data_{safe_folder}"


def process_file(conn, folder_name, filename, file_url, imported_files):
    """
    Download, extract, and import a single .7z file.
    Returns (table_name, row_count) or None.
    """
    # Check if already imported
    file_hash = hashlib.sha256(file_url.encode()).hexdigest()
    if file_hash in imported_files:
        log(f"  Skipping {filename} (already imported)")
        return None

    table_name = generate_table_name(folder_name, filename)
    log(f"Processing {folder_name}/{filename} -> {table_name}")

    work = os.path.join(WORK_DIR, folder_name, filename.replace('.7z', ''))
    os.makedirs(work, exist_ok=True)

    try:
        archive_path = os.path.join(work, filename)
        txt_path = None

        for attempt in range(1, DOWNLOAD_RETRIES + 1):
            if os.path.exists(archive_path):
                os.remove(archive_path)

            extract_dir = os.path.join(work, "extracted")
            shutil.rmtree(extract_dir, ignore_errors=True)
            os.makedirs(extract_dir, exist_ok=True)

            log(f"  Attempt {attempt}/{DOWNLOAD_RETRIES}")
            download_file(file_url, archive_path)

            ok, detail = test_7z_archive(archive_path)
            if not ok:
                log(f"  Warning: {detail}")
                if attempt < DOWNLOAD_RETRIES:
                    wait = min(2 ** attempt + random.uniform(0, 1), 30)
                    log(f"  Retrying download in {wait:.0f}s due to archive validation failure...")
                    time.sleep(wait)
                    continue
                raise RuntimeError(detail)

            try:
                txt_path = extract_7z(archive_path, extract_dir)
                break
            except Exception as e:
                log(f"  Warning: extract failed on attempt {attempt}: {e}")
                if attempt < DOWNLOAD_RETRIES:
                    wait = min(2 ** attempt + random.uniform(0, 1), 30)
                    log(f"  Retrying with fresh download in {wait:.0f}s...")
                    time.sleep(wait)
                    continue
                raise

        if not txt_path:
            log(f"  No .txt file found in archive")
            return None
        log(f"  Extracted: {os.path.basename(txt_path)}")

        # Import
        row_count = import_file(conn, table_name, txt_path)

        # Ensure fast search indexes
        ensure_search_indexes(conn, table_name)

        # Grant read access to webapp user
        grant_select(conn, table_name)

        # Record in new file tracker
        record_file_import(conn, file_url, folder_name, table_name, row_count)
        
        # Also update old tracker for backward compatibility (gebruik folder_name als day_num placeholder)
        # Probeer day nummer te extraheren, anders gebruik hash
        day_match = re.search(r'day[_-]?(\d+)', folder_name, re.IGNORECASE)
        if day_match:
            day_num = int(day_match.group(1))
            record_import(conn, day_num, folder_name, table_name, row_count)
        
        log(f"  {folder_name}/{filename} complete: {row_count:,} rows in {table_name}")

        return table_name, row_count

    except Exception as e:
        log(f"  ERROR processing {folder_name}/{filename}: {e}")
        try:
            if conn and conn.closed == 0:
                conn.rollback()
        except Exception:
            pass
        return None
    finally:
        # Cleanup temp files
        shutil.rmtree(work, ignore_errors=True)


def run_check(conn):
    """
    Check for new folders and files, import them.
    Nieuwe flexible versie: werkt met alle folder namen en meerdere .7z per folder.
    """
    log("Checking for new data...")
    imported_files = get_imported_files(conn)
    folders = discover_folders()

    if not folders:
        log("No folders found or source unreachable")
        return

    log(f"Source has {len(folders)} folder(s)")

    # Voor elke folder, vind alle .7z bestanden
    new_files = []
    for folder_name, folder_url in folders:
        files = find_all_7z_in_folder(folder_url)
        if not files:
            log(f"  No .7z files in {folder_name}")
            continue
        
        log(f"  Found {len(files)} .7z file(s) in {folder_name}: {[f[0] for f in files]}")
        
        for filename, file_url in files:
            file_hash = hashlib.sha256(file_url.encode()).hexdigest()
            if file_hash not in imported_files:
                new_files.append((folder_name, filename, file_url))

    if not new_files:
        log("No new files to import")
        return

    log(f"Found {len(new_files)} new file(s) to process")

    # Process elk nieuw bestand
    for folder_name, filename, file_url in new_files:
        result = process_file(conn, folder_name, filename, file_url, imported_files)
        if result:
            table_name, row_count = result
            file_hash = hashlib.sha256(file_url.encode()).hexdigest()
            imported_files.add(file_hash)


def run_check_once():
    with _fetch_lock:
        if _fetch_state["running"]:
            return False
        _fetch_state["running"] = True
        _fetch_state["started_at"] = datetime.now(timezone.utc).isoformat()
        _fetch_state["finished_at"] = None
        _fetch_state["progress"] = "Bezig met checken op nieuwe data..."
        _fetch_state["error"] = None
        _fetch_state["files_imported"] = 0
        _fetch_state["total_rows"] = 0
        _fetch_state["log"] = []

    conn = None
    try:
        os.makedirs(WORK_DIR, exist_ok=True)
        log("Manual fetch started")

        for _ in range(30):
            try:
                conn = connect()
                break
            except Exception:
                time.sleep(2)

        if not conn:
            raise RuntimeError("Kon geen verbinding maken met database")

        get_imported_days(conn)
        get_imported_files(conn)
        ensure_webapp_user(conn)
        grant_select_tracker(conn)

        cur = conn.cursor()
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND "
            "(table_name LIKE 'accounts_%%' OR table_name LIKE 'data_%%')"
        )
        for (tbl,) in cur.fetchall():
            grant_select(conn, tbl)
            ensure_search_indexes(conn, tbl)
        cur.close()

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*), COALESCE(SUM(row_count), 0) FROM _file_tracker")
        before_files, before_rows = cur.fetchone()
        cur.close()

        run_check(conn)

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*), COALESCE(SUM(row_count), 0) FROM _file_tracker")
        after_files, after_rows = cur.fetchone()
        cur.close()

        with _fetch_lock:
            _fetch_state["progress"] = "Klaar"
            _fetch_state["files_imported"] = max((after_files or 0) - (before_files or 0), 0)
            _fetch_state["total_rows"] = max((after_rows or 0) - (before_rows or 0), 0)
        log("Manual fetch finished")
    except Exception as e:
        log(f"ERROR in manual fetch: {e}")
        with _fetch_lock:
            _fetch_state["error"] = str(e)
            _fetch_state["progress"] = "Fout"
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        with _fetch_lock:
            _fetch_state["running"] = False
            _fetch_state["finished_at"] = datetime.now(timezone.utc).isoformat()


def start_manual_fetch_thread():
    with _fetch_lock:
        if _fetch_state["running"]:
            return False
    t = threading.Thread(target=run_check_once, daemon=True)
    t.start()
    return True


def get_fetch_status():
    with _fetch_lock:
        return dict(_fetch_state)


api_app = Flask(__name__)


@api_app.route("/status", methods=["GET"])
def api_status():
    return jsonify(get_fetch_status())


@api_app.route("/healthz", methods=["GET"])
def api_healthz():
    return "ok", 200


@api_app.route("/fetch", methods=["POST"])
def api_fetch():
    started = start_manual_fetch_thread()
    if started:
        return jsonify({"ok": True, "message": "Fetch gestart"})
    return jsonify({"ok": False, "error": "Er draait al een fetch"}), 409


def run_manual_api_server():
    log(f"Starting fetcher API on :{FETCHER_API_PORT} (manual mode, gunicorn)")
    import gunicorn.app.base

    class StandaloneApp(gunicorn.app.base.BaseApplication):
        def __init__(self, app, options=None):
            self.options = options or {}
            self.application = app
            super().__init__()

        def load_config(self):
            for key, value in self.options.items():
                self.cfg.set(key.lower(), value)

        def load(self):
            return self.application

    StandaloneApp(api_app, {
        "bind": f"0.0.0.0:{FETCHER_API_PORT}",
        "workers": 1,
        "threads": 2,
        "timeout": 30,
        "accesslog": "-",
        "loglevel": "warning",
    }).run()


def main():
    if MANUAL_MODE:
        # Ensure the read-only webapp user exists before the web container tries to connect
        log("MANUAL_MODE: ensuring webapp user and DB setup before starting API...")
        for attempt in range(30):
            try:
                _boot_conn = connect()
                log("Connected to database")
                break
            except Exception:
                time.sleep(2)
        else:
            log("ERROR: Could not connect to database after 60s, starting API anyway")
            _boot_conn = None

        if _boot_conn:
            try:
                get_imported_days(_boot_conn)
                get_imported_files(_boot_conn)
                ensure_webapp_user(_boot_conn)
                grant_select_tracker(_boot_conn)
                cur = _boot_conn.cursor()
                cur.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND "
                    "(table_name LIKE 'accounts_%%' OR table_name LIKE 'data_%%')"
                )
                for (tbl,) in cur.fetchall():
                    grant_select(_boot_conn, tbl)
                cur.close()
                log("MANUAL_MODE: webapp user and permissions OK")
            except Exception as e:
                log(f"MANUAL_MODE: warning during boot setup: {e}")
            finally:
                try:
                    _boot_conn.close()
                except Exception:
                    pass

        run_manual_api_server()
        return

    os.makedirs(WORK_DIR, exist_ok=True)

    # Wait for DB to be ready
    log("Waiting for database...")
    for attempt in range(30):
        try:
            conn = connect()
            log("Connected to database")
            break
        except Exception:
            time.sleep(2)
    else:
        log("ERROR: Could not connect to database after 60s")
        sys.exit(1)

    # Ensure tracking tables exist (both old and new)
    get_imported_days(conn)
    get_imported_files(conn)

    # Ensure webapp read-only user exists and has base permissions
    ensure_webapp_user(conn)
    grant_select_tracker(conn)

    # Grant SELECT on any existing data tables (accounts_* en data_*)
    cur = conn.cursor()
    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' AND "
        "(table_name LIKE 'accounts_%%' OR table_name LIKE 'data_%%')"
    )
    for (tbl,) in cur.fetchall():
        grant_select(conn, tbl)
        ensure_search_indexes(conn, tbl)
    cur.close()

    # Check if DB has data
    has_data = False
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM _file_tracker;")
        file_count = cur.fetchone()[0]
        if file_count > 0:
            has_data = True
    except Exception:
        pass
    cur.close()

    if not has_data:
        log("Database is empty — bootstrapping from source...")
        run_check(conn)
    else:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM _file_tracker;")
        file_count = cur.fetchone()[0]
        cur.close()
        log(f"Database has {file_count} file(s) imported")

    # Periodic check loop
    log(f"Entering check loop (every {CHECK_INTERVAL}s / {CHECK_INTERVAL // 3600}h)")
    while True:
        time.sleep(CHECK_INTERVAL)
        try:
            # Reconnect in case connection was lost
            try:
                conn.cursor().execute("SELECT 1")
            except Exception:
                conn = connect()
            run_check(conn)
        except Exception as e:
            log(f"ERROR in check loop: {e}")
            try:
                conn = connect()
            except Exception:
                log("Could not reconnect to database")


if __name__ == "__main__":
    main()

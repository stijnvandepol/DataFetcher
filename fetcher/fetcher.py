"""
Data fetcher – checks for new folders, downloads ALL .7z archives,
extracts the NDJSON .txt, imports into PostgreSQL, notifies Discord.

FLEXIBLE: Works with any folder naming (not just 'day*') and handles
multiple .7z files per folder. Tracks by file URL hash to prevent duplicates.

Runs once on startup (bootstrap if DB empty) then every CHECK_INTERVAL.
"""

import os
import re
import sys
import time
import shutil
import hashlib
import tempfile
import subprocess
import json
from datetime import datetime, timezone

import requests
import psycopg2
import orjson

# ── Config ──────────────────────────────────────────────────────
sys.stdout.reconfigure(line_buffering=True)

SOURCE_URL = os.getenv("SOURCE_URL", "http://37.72.140.17/pay_or_leak/odido/")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "21600"))  # 6 hours

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "salesforce")
DB_USER = os.getenv("DB_USER", "stijn")
DB_PASSWORD = os.getenv("DB_PASSWORD", "supersecure123")
WEBAPP_USER = os.getenv("WEBAPP_USER", "webapp")
WEBAPP_PASSWORD = os.getenv("WEBAPP_PASSWORD", "webapp")

ID_FIELD = "Id"
BATCH_SIZE = 5000
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
]


# ── Logging ─────────────────────────────────────────────────────
def log(msg: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


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
        cur.execute(f'GRANT SELECT ON "{table_name}" TO {WEBAPP_USER};')
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
        cur.execute(f'GRANT SELECT ON _import_tracker TO {WEBAPP_USER};')
        cur.execute(f'GRANT SELECT ON _file_tracker TO {WEBAPP_USER};')
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
            cur.execute(f'CREATE USER {WEBAPP_USER} WITH PASSWORD %s;', (WEBAPP_PASSWORD,))
            log(f"Created database user '{WEBAPP_USER}'")
        cur.execute(f'GRANT CONNECT ON DATABASE {DB_NAME} TO {WEBAPP_USER};')
        cur.execute(f'GRANT USAGE ON SCHEMA public TO {WEBAPP_USER};')
        # Grant SELECT on information_schema so web app can discover columns
        cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA information_schema TO {WEBAPP_USER};')
        # Grant SELECT on all existing public tables
        cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA public TO {WEBAPP_USER};')
        # Auto-grant SELECT on future tables created by this user
        cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {WEBAPP_USER};')
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
                f'CREATE INDEX IF NOT EXISTS "{trigram_idx}" '
                f'ON "{table_name}" USING GIN ("{col}" gin_trgm_ops);'
            )
            cur.execute(
                f'CREATE INDEX IF NOT EXISTS "{prefix_idx}" '
                f'ON "{table_name}" ((LOWER("{col}")));'
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
        cur.execute(f'ALTER TABLE "{table}" ADD COLUMN "{k}" TEXT;')
    return existing | set(missing)


def _flush_batch(cur, table, cols, batch):
    if not batch:
        return
    col_sql = ", ".join([f'"{c}"' for c in cols])
    placeholders = ", ".join(["%s"] * len(cols))
    # ON CONFLICT DO NOTHING voorkomt duplicaten bij herstart
    id_idx = None
    for i, c in enumerate(cols):
        if c == "Id":
            id_idx = i
            break
    if id_idx is not None:
        query = f'INSERT INTO "{table}" ({col_sql}) VALUES ({placeholders}) ON CONFLICT ("Id") DO NOTHING'
    else:
        query = f'INSERT INTO "{table}" ({col_sql}) VALUES ({placeholders})'
    cur.executemany(query, batch)


def import_file(conn, table_name, file_path):
    """Import an NDJSON .txt file into the given table. Returns row count."""
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    log(f"  Importing {os.path.basename(file_path)} ({file_size_mb:.1f} MB) -> {table_name}")
    t0 = time.time()

    known_keys = set(infer_keys(file_path))
    cur = conn.cursor()

    # Create table
    cols_sql = ", ".join([f'"{k}" TEXT' for k in known_keys])
    cur.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({cols_sql});')
    # Ensure unique constraint on Id to prevent duplicates
    cur.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS "idx_{table_name}_id_uniq" ON "{table_name}"("Id");')
    conn.commit()

    known_keys = get_existing_columns(cur, table_name)
    inserted = 0
    batch_cols = None
    batch_values = []

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

            cols = tuple(sorted(known_keys))
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
        cur.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_{ID_FIELD}" ON "{table_name}"("{ID_FIELD}");')
    if "Name" in known_keys:
        cur.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_Name" ON "{table_name}"("Name");')
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
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
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


# ── Discord notification ────────────────────────────────────────
def notify_discord(day_num, table_name, row_count, total_days):
    if not DISCORD_WEBHOOK:
        return
    embed = {
        "title": "📦 Nieuwe data geïmporteerd",
        "color": 3066993,  # green
        "fields": [
            {"name": "Day", "value": f"day{day_num}", "inline": True},
            {"name": "Tabel", "value": table_name, "inline": True},
            {"name": "Rijen", "value": f"{row_count:,}", "inline": True},
            {"name": "Totaal days", "value": str(total_days), "inline": True},
        ],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": "Data Fetcher"},
    }
    payload = {"embeds": [embed]}
    try:
        resp = requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)
        if resp.status_code in (200, 204):
            log(f"  Discord notificatie verstuurd")
        else:
            log(f"  Discord fout: {resp.status_code}")
    except Exception as e:
        log(f"  Discord fout: {e}")


def notify_discord_file(folder_name, filename, table_name, row_count):
    """Nieuwe Discord notificatie voor flexible file imports."""
    if not DISCORD_WEBHOOK:
        return
    embed = {
        "title": "📦 Nieuwe data geïmporteerd",
        "color": 3066993,  # green
        "fields": [
            {"name": "Folder", "value": folder_name, "inline": True},
            {"name": "Bestand", "value": filename, "inline": True},
            {"name": "Tabel", "value": table_name, "inline": True},
            {"name": "Rijen", "value": f"{row_count:,}", "inline": True},
        ],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": "Data Fetcher"},
    }
    payload = {"embeds": [embed]}
    try:
        resp = requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)
        if resp.status_code in (200, 204):
            log(f"  Discord notificatie verstuurd")
        else:
            log(f"  Discord fout: {resp.status_code}")
    except Exception as e:
        log(f"  Discord fout: {e}")


def notify_discord_startup(total_files, total_rows):
    """Aangepaste startup notificatie die files telt i.p.v. days."""
    if not DISCORD_WEBHOOK:
        return
    embed = {
        "title": "🚀 Fetcher opgestart",
        "color": 3447003,  # blue
        "description": f"Database bevat **{total_files}** geïmporteerde bestanden met **{total_rows:,}** rijen totaal.\nControleert elke **{CHECK_INTERVAL // 3600}** uur op nieuwe data.",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": "Data Fetcher"},
    }
    try:
        requests.post(DISCORD_WEBHOOK, json={"embeds": [embed]}, timeout=10)
    except Exception:
        pass


# ── Main logic ──────────────────────────────────────────────────
def generate_table_name(folder_name, filename):
    """
    Generate a safe PostgreSQL table name from folder and filename.
    Example: folder='day-3', filename='accounts.7z' -> 'data_day_3_accounts'
    """
    # Verwijder extensie
    base = os.path.splitext(filename)[0]
    # Combineer folder + filename
    combined = f"{folder_name}_{base}"
    # Maak PostgreSQL-safe: lowercase, vervang niet-alphanumerisch met underscore
    safe = re.sub(r'[^a-z0-9]+', '_', combined.lower())
    # Verwijder leading/trailing underscores
    safe = safe.strip('_')
    # Prefix met 'data_' voor duidelijkheid
    return f"data_{safe}"


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
                    log("  Retrying download due to archive validation failure...")
                    continue
                raise RuntimeError(detail)

            try:
                txt_path = extract_7z(archive_path, extract_dir)
                break
            except Exception as e:
                log(f"  Warning: extract failed on attempt {attempt}: {e}")
                if attempt < DOWNLOAD_RETRIES:
                    log("  Retrying with fresh download...")
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


def process_day(conn, day_num, day_url, imported_days):
    """
    DEPRECATED: oude functie voor backward compatibility.
    Gebruik process_folder() voor nieuwe flexibele flow.
    """
    table_name = f"accounts_{day_num}"
    log(f"Processing day{day_num} -> {table_name}")

    # Find .7z URLs (kan er meerdere zijn nu!)
    files = find_all_7z_in_folder(day_url)
    if not files:
        log(f"  No .7z files found in {day_url}")
        return None

    # Process alleen het eerste bestand voor backward compatibility
    filename, archive_url = files[0]
    
    work = os.path.join(WORK_DIR, f"day{day_num}")
    os.makedirs(work, exist_ok=True)

    try:
        archive_path = os.path.join(work, f"day{day_num}.7z")
        txt_path = None

        for attempt in range(1, DOWNLOAD_RETRIES + 1):
            if os.path.exists(archive_path):
                os.remove(archive_path)

            extract_dir = os.path.join(work, "extracted")
            shutil.rmtree(extract_dir, ignore_errors=True)
            os.makedirs(extract_dir, exist_ok=True)

            log(f"  Attempt {attempt}/{DOWNLOAD_RETRIES}")
            download_file(archive_url, archive_path)

            ok, detail = test_7z_archive(archive_path)
            if not ok:
                log(f"  Warning: {detail}")
                if attempt < DOWNLOAD_RETRIES:
                    log("  Retrying download due to archive validation failure...")
                    continue
                raise RuntimeError(detail)

            try:
                txt_path = extract_7z(archive_path, extract_dir)
                break
            except Exception as e:
                log(f"  Warning: extract failed on attempt {attempt}: {e}")
                if attempt < DOWNLOAD_RETRIES:
                    log("  Retrying with fresh download...")
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

        # Record
        record_import(conn, day_num, f"day{day_num}", table_name, row_count)
        log(f"  day{day_num} complete: {row_count:,} rows in {table_name}")

        return table_name, row_count

    except Exception as e:
        log(f"  ERROR processing day{day_num}: {e}")
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
            
            # Discord notificatie
            notify_discord_file(folder_name, filename, table_name, row_count)


def run_check_legacy(conn):
    """
    DEPRECATED: Oude check functie die alleen 'day*' folders zoekt.
    Bewaard voor backward compatibility, maar gebruik run_check() voor nieuwe installaties.
    """
    log("Checking for new data (legacy mode)...")
    imported_days = get_imported_days(conn)
    
    # Probeer oude day-pattern te vinden
    try:
        resp = requests.get(SOURCE_URL, timeout=30)
        resp.raise_for_status()
        pattern = re.compile(r'href="(day(\d+)/)"', re.IGNORECASE)
        available_days = []
        for match in pattern.finditer(resp.text):
            day_path, day_num = match.group(1), int(match.group(2))
            day_url = SOURCE_URL.rstrip("/") + "/" + day_path
            available_days.append((day_num, day_url))
        available_days.sort(key=lambda x: x[0])
    except Exception as e:
        log(f"Error in legacy check: {e}")
        return

    if not available_days:
        log("No days found or source unreachable")
        return

    log(f"Source has {len(available_days)} day(s), we have {len(imported_days)} imported")

    new_days = [(num, url) for num, url in available_days if num not in imported_days]
    if not new_days:
        log("No new data available")
        return

    log(f"Found {len(new_days)} new day(s): {[f'day{n}' for n, _ in new_days]}")

    for day_num, day_url in new_days:
        result = process_day(conn, day_num, day_url, imported_days)
        if result:
            table_name, row_count = result
            imported_days.add(day_num)
            notify_discord(day_num, table_name, row_count, len(imported_days))


def main():
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

    # Notify startup
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*), COALESCE(SUM(row_count), 0) FROM _file_tracker;")
    result = cur.fetchone()
    total_files = result[0] if result else 0
    total_rows = result[1] if result else 0
    cur.close()
    notify_discord_startup(total_files, total_rows)

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

"""
Data fetcher – manual API server for on-demand 7z imports into PostgreSQL.

Runs a gunicorn API server that accepts direct 7z URLs with custom database names.
Downloads, extracts, and imports data while tracking imports to prevent duplicates.
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
import psycopg2.extras
import psycopg2.sql as sql
from psycopg2.extras import execute_values
import orjson
from flask import Flask, jsonify, request

# ── Config ──────────────────────────────────────────────────────
sys.stdout.reconfigure(line_buffering=True)

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
LOCAL_IMPORT_ROOT = os.path.realpath(os.getenv("LOCAL_IMPORT_ROOT", "/data/incoming"))

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
    "manual_url": None,
    "manual_db_name": None,
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


def set_progress(message: str):
    with _fetch_lock:
        _fetch_state["progress"] = message


def is_remote_source(source: str) -> bool:
    return source.startswith("http://") or source.startswith("https://")


def resolve_local_source_path(source: str) -> str:
    local_source = source[len("file://"):] if source.startswith("file://") else source
    if os.path.isabs(local_source):
        resolved = os.path.realpath(local_source)
    else:
        resolved = os.path.realpath(os.path.join(LOCAL_IMPORT_ROOT, local_source))

    allowed_prefix = LOCAL_IMPORT_ROOT.rstrip(os.sep) + os.sep
    if resolved != LOCAL_IMPORT_ROOT and not resolved.startswith(allowed_prefix):
        raise ValueError(f"Local path is outside allowed import root: {LOCAL_IMPORT_ROOT}")
    if not os.path.isfile(resolved):
        raise ValueError(f"Local file not found: {resolved}")
    if not resolved.lower().endswith(".7z"):
        raise ValueError(f"Local file must be a .7z archive: {resolved}")
    return resolved


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
def download_file(url, dest_path):
    """Download a file with progress logging."""
    log(f"  Downloading {url}")
    resp = requests.get(url, stream=True, timeout=600)
    resp.raise_for_status()
    total = int(resp.headers.get("content-length", 0))
    downloaded = 0
    last_log = 0
    next_pct_log = 5

    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=4 * 1024 * 1024):
            if not chunk:
                continue
            f.write(chunk)
            downloaded += len(chunk)
            if total:
                pct = (downloaded / total) * 100
                if pct >= next_pct_log or downloaded >= total:
                    progress_line = f"Download: {downloaded / (1024*1024):.0f} MB / {total / (1024*1024):.0f} MB ({pct:.0f}%)"
                    set_progress(progress_line)
                    log(f"    {progress_line}")
                    while next_pct_log <= pct:
                        next_pct_log += 5
            else:
                # Fallback when server does not provide content-length
                log_interval = 10 * 1024 * 1024
                if downloaded - last_log > log_interval:
                    progress_line = f"Download: {downloaded / (1024*1024):.0f} MB"
                    set_progress(progress_line)
                    log(f"    {progress_line}")
                    last_log = downloaded

    size_mb = os.path.getsize(dest_path) / (1024 * 1024)
    log(f"  ✓ Download voltooid: {size_mb:.1f} MB")


def extract_7z(archive_path, extract_dir):
    """Extract .7z using 7z command line tool. Returns path to first .txt found."""
    log(f"  Archief aan het uitpakken...")
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
                log(f"  ✓ Uitpakken voltooid: gevonden {f}")
                return os.path.join(root, f)
    # Fallback: any .txt
    for root, dirs, files in os.walk(extract_dir):
        for f in files:
            if f.endswith(".txt"):
                log(f"  ✓ Uitpakken voltooid: gevonden {f}")
                return os.path.join(root, f)
    return None


def test_7z_archive(archive_path):
    """Return (ok, detail) after running a 7z integrity test."""
    log(f"  Archief aan het valideren...")
    result = subprocess.run(
        ["7z", "t", archive_path],
        check=False, capture_output=True, text=True,
    )
    if result.returncode == 0:
        log(f"  ✓ Validatie geslaagd")
        return True, "ok"

    stderr = (result.stderr or "").strip()
    stdout = (result.stdout or "").strip()
    detail = stderr if stderr else stdout
    detail = detail[-800:] if detail else "no output"
    return False, f"7z test failed (exit={result.returncode}): {detail}"


# ── Main logic ──────────────────────────────────────────────────
def process_file_direct(conn, source, db_name):
    """
    Process a single .7z file directly from a URL or local path with custom database name.
    Returns (table_name, row_count) or None.
    """
    source = source.strip()
    remote = is_remote_source(source)
    local_path = None
    if not remote:
        local_path = resolve_local_source_path(source)

    tracking_key = source if remote else f"local:{local_path}"
    file_hash = hashlib.sha256(tracking_key.encode()).hexdigest()
    imported_files = get_imported_files(conn)
    
    if file_hash in imported_files:
        log(f"  File already imported (hash: {file_hash[:16]}...)")
        return None

    if remote:
        filename = source.rstrip("/").split("/")[-1]
    else:
        filename = os.path.basename(local_path)

    if not filename.lower().endswith(".7z"):
        raise ValueError(f"Source does not point to a .7z file: {filename}")

    table_name = db_name.lower().replace("-", "_").replace(" ", "_")
    if not table_name:
        raise ValueError("Database name cannot be empty")
    
    log(f"Processing {filename} -> {table_name}")
    set_progress(f"Voorbereiden: {filename}")

    work = os.path.join(WORK_DIR, table_name)
    os.makedirs(work, exist_ok=True)

    try:
        archive_path = os.path.join(work, filename)
        txt_path = None

        max_attempts = DOWNLOAD_RETRIES if remote else 1
        for attempt in range(1, max_attempts + 1):
            if os.path.exists(archive_path):
                os.remove(archive_path)

            extract_dir = os.path.join(work, "extracted")
            shutil.rmtree(extract_dir, ignore_errors=True)
            os.makedirs(extract_dir, exist_ok=True)

            log(f"  Attempt {attempt}/{max_attempts}")
            if remote:
                set_progress(f"Download attempt {attempt}/{max_attempts}: {filename}")
                download_file(source, archive_path)
            else:
                set_progress(f"Lokale file kopiëren: {filename}")
                log(f"  Using local file: {local_path}")
                shutil.copy2(local_path, archive_path)
                size_mb = os.path.getsize(archive_path) / (1024 * 1024)
                log(f"  ✓ Lokale file gekopieerd: {size_mb:.1f} MB")

            ok, detail = test_7z_archive(archive_path)
            if not ok:
                log(f"  Warning: {detail}")
                if attempt < max_attempts:
                    wait = min(2 ** attempt + random.uniform(0, 1), 30)
                    if remote:
                        log(f"  Retrying download in {wait:.0f}s due to archive validation failure...")
                    else:
                        log(f"  Retrying local copy in {wait:.0f}s due to archive validation failure...")
                    time.sleep(wait)
                    continue
                raise RuntimeError(detail)

            try:
                set_progress(f"Uitpakken: {filename}")
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
            log(f"  ERROR: Geen .txt bestand gevonden in archief")
            return None

        # Import
        log(f"  Data aan het importeren...")
        set_progress(f"Importeren in tabel {table_name}...")
        row_count = import_file(conn, table_name, txt_path)

        # Ensure fast search indexes
        log(f"  Indexen aan het aanmaken...")
        set_progress(f"Indexen maken voor {table_name}...")
        ensure_search_indexes(conn, table_name)

        # Grant read access to webapp user
        log(f"  Permissies aan het instellen...")
        set_progress(f"Permissies instellen voor {table_name}...")
        grant_select(conn, table_name)

        # Record in file tracker
        log(f"  Bestand aan het registreren...")
        set_progress(f"Registreren van {filename}...")
        record_file_import(conn, tracking_key, table_name, table_name, row_count)
        
        log(f"  ✓ KLAAR: {row_count:,} rows geïmporteerd in tabel '{table_name}'")

        return table_name, row_count

    except Exception as e:
        log(f"  ERROR processing {filename}: {e}")
        try:
            if conn and conn.closed == 0:
                conn.rollback()
        except Exception:
            pass
        return None
    finally:
        # Cleanup temp files
        shutil.rmtree(work, ignore_errors=True)





def run_check_once():
    with _fetch_lock:
        if _fetch_state["running"]:
            return False
        _fetch_state["running"] = True
        _fetch_state["started_at"] = datetime.now(timezone.utc).isoformat()
        _fetch_state["finished_at"] = None
        _fetch_state["progress"] = "Bezig..."
        _fetch_state["error"] = None
        _fetch_state["files_imported"] = 0
        _fetch_state["total_rows"] = 0
        _fetch_state["log"] = []
        
        # Check if manual URL is set
        manual_url = _fetch_state.get("manual_url")
        manual_db_name = _fetch_state.get("manual_db_name")

    conn = None
    try:
        os.makedirs(WORK_DIR, exist_ok=True)
        
        if manual_url:
            log(f"Manual fetch with source: {manual_url}")
            log(f"Target database: {manual_db_name}")
        else:
            log("Automatic check started")

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

        # Decide which mode to run
        if manual_url and manual_db_name:
            with _fetch_lock:
                _fetch_state["progress"] = "Bezig met downloaden en importeren..."
            result = process_file_direct(conn, manual_url, manual_db_name)
            if result:
                table_name, row_count = result
                with _fetch_lock:
                    _fetch_state["files_imported"] = 1
                    _fetch_state["total_rows"] = row_count
        else:
            with _fetch_lock:
                _fetch_state["progress"] = "Bezig met checken op nieuwe data..."
            run_check(conn)
            
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*), COALESCE(SUM(row_count), 0) FROM _file_tracker")
            after_files, after_rows = cur.fetchone()
            cur.close()

            with _fetch_lock:
                _fetch_state["files_imported"] = max((after_files or 0) - (before_files or 0), 0)
                _fetch_state["total_rows"] = max((after_rows or 0) - (before_rows or 0), 0)
        
        with _fetch_lock:
            _fetch_state["progress"] = "Klaar"
            _fetch_state["manual_url"] = None
            _fetch_state["manual_db_name"] = None

        log("Fetch finished")
    except Exception as e:
        log(f"ERROR: {e}")
        with _fetch_lock:
            _fetch_state["error"] = str(e)
            _fetch_state["progress"] = "Fout"
            _fetch_state["manual_url"] = None
            _fetch_state["manual_db_name"] = None
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
    """
    Start a fetch. Can be called in two ways:
    1. No parameters: automatic discovery (old behavior)
     2. With JSON body: {"file_url": "...", "db_name": "..."} for direct 7z import
         `file_url` supports both http(s) URLs and local paths under LOCAL_IMPORT_ROOT.
    """
    data = request.get_json(silent=True) or {}
    file_url = data.get("file_url", "").strip()
    db_name = data.get("db_name", "").strip()
    
    # Validate if both provided
    if file_url and not db_name:
        return jsonify({"ok": False, "error": "db_name is required when file_url is provided"}), 400
    if db_name and not file_url:
        return jsonify({"ok": False, "error": "file_url is required when db_name is provided"}), 400
    
    with _fetch_lock:
        if _fetch_state["running"]:
            return jsonify({"ok": False, "error": "Er draait al een fetch"}), 409
        
        # Set manual parameters if provided
        if file_url:
            _fetch_state["manual_url"] = file_url
            _fetch_state["manual_db_name"] = db_name
    
    started = start_manual_fetch_thread()
    if started:
        mode = "direct" if file_url else "auto"
        return jsonify({"ok": True, "message": f"Fetch gestart ({mode} mode)"})
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
    # Always run in manual API mode
    log("Starting fetcher in manual API mode...")
    log("Ensuring webapp user and DB setup before starting API...")
    
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
            log("Database setup OK, starting API server...")
        except Exception as e:
            log(f"Warning during boot setup: {e}")
        finally:
            try:
                _boot_conn.close()
            except Exception:
                pass

    run_manual_api_server()


if __name__ == "__main__":
    main()

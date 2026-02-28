"""
Fetcher library – importable by Flask web app.
Discovers folders, downloads .7z archives, extracts NDJSON, imports into PostgreSQL.
Runs in a background thread, reports status via shared state.
"""

import os
import re
import sys
import time
import shutil
import hashlib
import subprocess
import threading
from datetime import datetime, timezone

import requests
import psycopg2
import orjson

# ── Shared status ───────────────────────────────────────────────
_fetch_lock = threading.Lock()
_fetch_status = {
    "running": False,
    "progress": "",
    "log": [],
    "started_at": None,
    "finished_at": None,
    "error": None,
    "files_imported": 0,
    "total_rows": 0,
}

ID_FIELD = "Id"
BATCH_SIZE = 5000
WORK_DIR = "/tmp/fetcher"
DOWNLOAD_RETRIES = 3

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


def get_status():
    """Return a copy of current fetch status."""
    with _fetch_lock:
        return dict(_fetch_status, log=list(_fetch_status["log"]))


def _log(msg: str):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with _fetch_lock:
        _fetch_status["log"].append(line)
        # Keep last 200 lines
        if len(_fetch_status["log"]) > 200:
            _fetch_status["log"] = _fetch_status["log"][-200:]


def _set_progress(msg: str):
    with _fetch_lock:
        _fetch_status["progress"] = msg


# ── Database helpers ────────────────────────────────────────────
def _connect(db_config):
    return psycopg2.connect(**db_config)


def _get_imported_files(conn):
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


def _record_file_import(conn, file_url, folder_name, table_name, row_count):
    cur = conn.cursor()
    file_hash = hashlib.sha256(file_url.encode()).hexdigest()
    cur.execute(
        "INSERT INTO _file_tracker (file_hash, file_url, folder_name, table_name, row_count) "
        "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (file_hash) DO NOTHING;",
        (file_hash, file_url, folder_name, table_name, row_count),
    )
    conn.commit()
    cur.close()


def _grant_select(conn, table_name, webapp_user):
    cur = conn.cursor()
    try:
        cur.execute(f'GRANT SELECT ON "{table_name}" TO {webapp_user};')
        conn.commit()
        _log(f"  Granted SELECT on {table_name} to {webapp_user}")
    except Exception as e:
        conn.rollback()
        _log(f"  Warning: could not grant SELECT: {e}")
    cur.close()


def _ensure_webapp_user(conn, webapp_user, webapp_password, db_name):
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s;", (webapp_user,))
        if not cur.fetchone():
            cur.execute(f'CREATE USER {webapp_user} WITH PASSWORD %s;', (webapp_password,))
            _log(f"Created database user '{webapp_user}'")
        cur.execute(f'GRANT CONNECT ON DATABASE {db_name} TO {webapp_user};')
        cur.execute(f'GRANT USAGE ON SCHEMA public TO {webapp_user};')
        cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA information_schema TO {webapp_user};')
        cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA public TO {webapp_user};')
        cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {webapp_user};')
        conn.commit()
    except Exception as e:
        conn.rollback()
        _log(f"Warning: could not ensure webapp user: {e}")
    cur.close()


def _idx_name(table_name, col_name, kind):
    digest = hashlib.md5(f"{table_name}:{col_name}:{kind}".encode()).hexdigest()[:10]
    return f"idx_{table_name}_{kind}_{digest}"


def _ensure_search_indexes(conn, table_name):
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
        _log(f"  Search indexes ensured for {table_name}")
    except Exception as e:
        conn.rollback()
        _log(f"  Warning: could not build search indexes: {e}")
    finally:
        cur.close()


# ── Import logic ────────────────────────────────────────────────
def _infer_keys(path):
    with open(path, "rb") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = orjson.loads(line)
            if isinstance(obj, dict):
                return list(obj.keys())
    raise ValueError(f"No JSON objects in {path}")


def _get_existing_columns(cur, table):
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = %s AND table_schema = 'public'",
        (table,),
    )
    return {row[0] for row in cur.fetchall()}


def _ensure_columns(cur, table, keys, existing):
    missing = [k for k in keys if k not in existing]
    for k in missing:
        cur.execute(f'ALTER TABLE "{table}" ADD COLUMN "{k}" TEXT;')
    return existing | set(missing)


def _flush_batch(cur, table, cols, batch):
    if not batch:
        return
    col_sql = ", ".join([f'"{c}"' for c in cols])
    placeholders = ", ".join(["%s"] * len(cols))
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


def _import_file(conn, table_name, file_path):
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    _log(f"  Importing {os.path.basename(file_path)} ({file_size_mb:.1f} MB) -> {table_name}")
    t0 = time.time()

    known_keys = set(_infer_keys(file_path))
    cur = conn.cursor()

    cols_sql = ", ".join([f'"{k}" TEXT' for k in known_keys])
    cur.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({cols_sql});')
    cur.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS "idx_{table_name}_id_uniq" ON "{table_name}"("Id");')
    conn.commit()

    known_keys = _get_existing_columns(cur, table_name)
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
                known_keys = _ensure_columns(cur, table_name, list(new_keys), known_keys)
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
                    _log(f"    {table_name}: {inserted:,} rows ({rate:,.0f}/sec)")
                    _set_progress(f"Importeren: {table_name} — {inserted:,} rijen")

    if batch_values:
        _flush_batch(cur, table_name, batch_cols, batch_values)
    conn.commit()

    if ID_FIELD in known_keys:
        cur.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_{ID_FIELD}" ON "{table_name}"("{ID_FIELD}");')
    if "Name" in known_keys:
        cur.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_Name" ON "{table_name}"("Name");')
    conn.commit()
    cur.close()

    elapsed = time.time() - t0
    rate = inserted / elapsed if elapsed > 0 else 0
    _log(f"  Done: {inserted:,} rows in {elapsed:.1f}s ({rate:,.0f}/sec)")
    return inserted


# ── Web scraping ────────────────────────────────────────────────
def _discover_folders(source_url):
    try:
        resp = requests.get(source_url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        _log(f"Error fetching index: {e}")
        return []

    pattern = re.compile(r'href="([^/"]+/)"', re.IGNORECASE)
    folders = []
    for match in pattern.finditer(resp.text):
        folder_path = match.group(1)
        folder_name = folder_path.rstrip("/")
        if folder_name in (".", "..", "") or folder_name.startswith("."):
            continue
        folder_url = source_url.rstrip("/") + "/" + folder_path
        folders.append((folder_name, folder_url))

    _log(f"Discovered {len(folders)} folder(s): {[f[0] for f in folders]}")
    return folders


def _find_all_7z_in_folder(folder_url):
    try:
        resp = requests.get(folder_url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        _log(f"  Error fetching {folder_url}: {e}")
        return []

    pattern = re.compile(r'href="([^"]+\.7z)"', re.IGNORECASE)
    files = []
    for match in pattern.finditer(resp.text):
        filename = match.group(1)
        if "/" in filename or filename.startswith(".."):
            continue
        full_url = folder_url.rstrip("/") + "/" + filename
        files.append((filename, full_url))
    return files


def _download_file(url, dest_path):
    _log(f"  Downloading {url}")
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
                _log(f"    {downloaded / (1024*1024):.0f} MB / {total / (1024*1024):.0f} MB ({pct:.0f}%)")
                _set_progress(f"Downloaden: {pct:.0f}%")
                last_log = downloaded

    size_mb = os.path.getsize(dest_path) / (1024 * 1024)
    _log(f"  Downloaded {size_mb:.1f} MB")


def _extract_7z(archive_path, extract_dir):
    _log(f"  Extracting {os.path.basename(archive_path)}")
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
    for root, dirs, files in os.walk(extract_dir):
        for f in files:
            if f.endswith(".txt") and not f.startswith("OPEN") and not f.startswith("This_Is"):
                return os.path.join(root, f)
    for root, dirs, files in os.walk(extract_dir):
        for f in files:
            if f.endswith(".txt"):
                return os.path.join(root, f)
    return None


def _test_7z_archive(archive_path):
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


def _generate_table_name(folder_name, filename):
    base = os.path.splitext(filename)[0]
    combined = f"{folder_name}_{base}"
    safe = re.sub(r'[^a-z0-9]+', '_', combined.lower())
    safe = safe.strip('_')
    return f"data_{safe}"


# ── Main fetch function ────────────────────────────────────────
def _process_file(conn, folder_name, filename, file_url, imported_files, webapp_user):
    file_hash = hashlib.sha256(file_url.encode()).hexdigest()
    if file_hash in imported_files:
        _log(f"  Skipping {filename} (already imported)")
        return None

    table_name = _generate_table_name(folder_name, filename)
    _log(f"Processing {folder_name}/{filename} -> {table_name}")
    _set_progress(f"Verwerken: {folder_name}/{filename}")

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

            _log(f"  Attempt {attempt}/{DOWNLOAD_RETRIES}")
            _download_file(file_url, archive_path)

            ok, detail = _test_7z_archive(archive_path)
            if not ok:
                _log(f"  Warning: {detail}")
                if attempt < DOWNLOAD_RETRIES:
                    _log("  Retrying download...")
                    continue
                raise RuntimeError(detail)

            try:
                txt_path = _extract_7z(archive_path, extract_dir)
                break
            except Exception as e:
                _log(f"  Warning: extract failed on attempt {attempt}: {e}")
                if attempt < DOWNLOAD_RETRIES:
                    continue
                raise

        if not txt_path:
            _log(f"  No .txt file found in archive")
            return None
        _log(f"  Extracted: {os.path.basename(txt_path)}")

        row_count = _import_file(conn, table_name, txt_path)
        _ensure_search_indexes(conn, table_name)
        _grant_select(conn, table_name, webapp_user)
        _record_file_import(conn, file_url, folder_name, table_name, row_count)

        _log(f"  {folder_name}/{filename} complete: {row_count:,} rows")

        return table_name, row_count

    except Exception as e:
        _log(f"  ERROR: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    finally:
        shutil.rmtree(work, ignore_errors=True)


def run_fetch(db_config, source_url, webapp_user, webapp_password, db_name):
    """
    Main entry point – discovers new files and imports them.
    Called from a background thread by the web app.
    Returns (files_imported, total_rows).
    """
    global _fetch_status

    with _fetch_lock:
        if _fetch_status["running"]:
            return 0, 0
        _fetch_status["running"] = True
        _fetch_status["progress"] = "Verbinden met database..."
        _fetch_status["log"] = []
        _fetch_status["started_at"] = datetime.now(timezone.utc).isoformat()
        _fetch_status["finished_at"] = None
        _fetch_status["error"] = None
        _fetch_status["files_imported"] = 0
        _fetch_status["total_rows"] = 0

    os.makedirs(WORK_DIR, exist_ok=True)
    files_imported = 0
    total_rows = 0

    try:
        conn = _connect(db_config)
        _log("Connected to database")

        # Ensure webapp user and tracker tables
        _ensure_webapp_user(conn, webapp_user, webapp_password, db_name)
        imported_files = _get_imported_files(conn)

        # Discover folders
        _set_progress("Zoeken naar nieuwe data...")
        _log("Checking for new data...")
        folders = _discover_folders(source_url)

        if not folders:
            _log("No folders found or source unreachable")
            _set_progress("Geen folders gevonden")
            return 0, 0

        # Find new files
        new_files = []
        for folder_name, folder_url in folders:
            _set_progress(f"Scannen: {folder_name}")
            files = _find_all_7z_in_folder(folder_url)
            if not files:
                continue
            _log(f"  Found {len(files)} .7z file(s) in {folder_name}")
            for filename, file_url in files:
                file_hash = hashlib.sha256(file_url.encode()).hexdigest()
                if file_hash not in imported_files:
                    new_files.append((folder_name, filename, file_url))

        if not new_files:
            _log("No new files to import")
            _set_progress("Geen nieuwe bestanden gevonden")
            return 0, 0

        _log(f"Found {len(new_files)} new file(s) to process")

        for i, (folder_name, filename, file_url) in enumerate(new_files, 1):
            _set_progress(f"Bestand {i}/{len(new_files)}: {folder_name}/{filename}")
            result = _process_file(conn, folder_name, filename, file_url, imported_files, webapp_user)
            if result:
                table_name, row_count = result
                file_hash = hashlib.sha256(file_url.encode()).hexdigest()
                imported_files.add(file_hash)
                files_imported += 1
                total_rows += row_count

                with _fetch_lock:
                    _fetch_status["files_imported"] = files_imported
                    _fetch_status["total_rows"] = total_rows

        conn.close()
        _log(f"Fetch complete: {files_imported} file(s), {total_rows:,} rows total")
        _set_progress(f"Klaar: {files_imported} bestand(en), {total_rows:,} rijen")
        return files_imported, total_rows

    except Exception as e:
        _log(f"FETCH ERROR: {e}")
        with _fetch_lock:
            _fetch_status["error"] = str(e)
        _set_progress(f"Fout: {e}")
        return files_imported, total_rows

    finally:
        with _fetch_lock:
            _fetch_status["running"] = False
            _fetch_status["finished_at"] = datetime.now(timezone.utc).isoformat()


def start_fetch_thread(db_config, source_url, webapp_user, webapp_password, db_name):
    """Start fetch in a background thread. Returns True if started, False if already running."""
    with _fetch_lock:
        if _fetch_status["running"]:
            return False

    t = threading.Thread(
        target=run_fetch,
        args=(db_config, source_url, webapp_user, webapp_password, db_name),
        daemon=True,
    )
    t.start()
    return True

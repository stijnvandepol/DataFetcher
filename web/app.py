import os
import time
import hashlib
import secrets
import functools
import logging
import json
from datetime import datetime
from flask import Flask, request, render_template, redirect, url_for, session, abort, make_response, jsonify
import requests
import psycopg2
import psycopg2.extras
import psycopg2.pool

# Suppress werkzeug access logs maar laat app errors door
logging.basicConfig(level=logging.WARNING)
logging.getLogger("werkzeug").setLevel(logging.CRITICAL)

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", secrets.token_hex(32))

# Secure session cookies
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=False,  # False voor HTTP, True voor HTTPS
    SESSION_COOKIE_NAME="__sid",           # Generieke cookie naam
    PERMANENT_SESSION_LIFETIME=3600,
)

# Wachtwoord gehashed
_raw_pw = os.environ["APP_PASSWORD"]
APP_PASSWORD_HASH = hashlib.sha256(_raw_pw.encode()).hexdigest()
del _raw_pw

# Admin wachtwoord (apart, voor admin panel)
_raw_admin = os.environ["ADMIN_PASSWORD"]
ADMIN_PASSWORD_HASH = hashlib.sha256(_raw_admin.encode()).hexdigest()
del _raw_admin

# Rate limiting
_login_attempts = {}
MAX_LOGIN_ATTEMPTS = 5
LOCKOUT_SECONDS = 300

# Sessie tracking
_active_sessions = {}  # session_id -> {ip, user_agent, last_seen, login_time}

# Audit log (in-memory, laatste 200)
_audit_log = []
MAX_AUDIT = 200

DB_CONFIG = {
    "host": os.environ["DB_HOST"],
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.environ["DB_NAME"],
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
}

# Connection pool: min 1, max 8 connections (gunicorn: 2 workers * 4 threads)
_db_pool = None


def _get_pool():
    global _db_pool
    if _db_pool is None or _db_pool.closed:
        _db_pool = psycopg2.pool.ThreadedConnectionPool(1, 8, **DB_CONFIG)
    return _db_pool


def get_db():
    return _get_pool().getconn()


def put_db(conn):
    """Return a connection to the pool (rollback if in error state)."""
    try:
        if conn and not conn.closed:
            _get_pool().putconn(conn)
    except Exception:
        pass

FETCHER_API_URL = os.getenv("FETCHER_API_URL", "http://fetcher:8000").rstrip("/")


# ── CSRF protection ────────────────────────────────────────────
def _get_csrf_token():
    """Return (and lazily create) a per-session CSRF token."""
    if "csrf_token" not in session:
        session["csrf_token"] = secrets.token_hex(32)
    return session["csrf_token"]


def _check_csrf():
    """Abort 403 if the submitted CSRF token doesn't match the session."""
    token = request.form.get("csrf_token") or request.headers.get("X-CSRF-Token", "")
    if not token or not secrets.compare_digest(token, _get_csrf_token()):
        abort(403)


@app.context_processor
def inject_csrf():
    """Make csrf_token available in all templates."""
    return {"csrf_token": _get_csrf_token}

# Kolommen die standaard AAN staan in de UI
DEFAULT_ON = {
    "Name", "Phone",
    "vlocity_cmt__BillingEmailAddress__c",
    "BillingStreet", "House_Number__c", "House_Number_Extension__c",
    "BillingPostalCode", "BillingCity",
    "Description", "Flash_Message__c",
    "Bank_Account_Number__c", "Bank_Account_Holder_Name__c",
    "Payment_Method__c", "Password__c",
}

# Beperkte set kolommen waarop gezocht mag worden (kolom -> Nederlands label)
SEARCHABLE_FIELDS = [
    ("Name", "Naam"),
    ("vlocity_cmt__BillingEmailAddress__c", "E-mailadres"),
    ("Phone", "Telefoon"),
    ("BillingCity", "Stad"),
    ("BillingPostalCode", "Postcode"),
    ("BillingStreet", "Straat"),
    ("House_Number__c", "Huisnummer"),
    ("Bank_Account_Number__c", "Bankrekening (IBAN)"),
    ("Bank_Account_Holder_Name__c", "Rekeninghouder"),
    ("Id", "Account ID"),
    ("Account_Salesforce_ID__c", "Salesforce ID"),
    ("ParentAccountName__c", "Bovenliggend account"),
    ("Description", "Omschrijving"),
    ("Flash_Message__c", "Flash Message"),
]

# Cache (met TTL zodat nieuwe tabellen automatisch zichtbaar worden)
_columns_cache = None
_columns_cache_time = 0
_tables_cache = None
_tables_cache_time = 0
CACHE_TTL = 30  # 30 seconden


def get_account_tables():
    """Discover all data_* or accounts_* tables dynamically."""
    global _tables_cache, _tables_cache_time
    if _tables_cache is not None and time.time() - _tables_cache_time < CACHE_TTL:
        return _tables_cache
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        # Use pg_catalog.pg_tables - visible to all users regardless of grants
        cur.execute(
            "SELECT tablename FROM pg_catalog.pg_tables "
            "WHERE schemaname = 'public' "
            "AND (tablename LIKE 'accounts_%%' OR tablename LIKE 'data_%%') "
            "ORDER BY tablename"
        )
        _tables_cache = [row[0] for row in cur.fetchall()]
        _tables_cache_time = time.time()
        cur.close()
        put_db(conn)
    except Exception as e:
        put_db(conn)
        print(f"[ERROR] get_account_tables: {e}", flush=True)
        _tables_cache = _tables_cache or []
    return _tables_cache


def get_all_columns():
    global _columns_cache, _columns_cache_time
    if _columns_cache is not None and time.time() - _columns_cache_time < CACHE_TTL:
        return _columns_cache
    conn = None
    try:
        tables = get_account_tables()
        if not tables:
            return []
        conn = get_db()
        cur = conn.cursor()
        # Use pg_catalog - always visible regardless of table grants
        cur.execute(
            "SELECT a.attname FROM pg_catalog.pg_attribute a "
            "JOIN pg_catalog.pg_class c ON a.attrelid = c.oid "
            "JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "
            "WHERE n.nspname = 'public' AND c.relname = %s "
            "AND a.attnum > 0 AND NOT a.attisdropped "
            "ORDER BY a.attname",
            (tables[0],)
        )
        _columns_cache = [row[0] for row in cur.fetchall()]
        _columns_cache_time = time.time()
        cur.close()
        put_db(conn)
    except Exception as e:
        put_db(conn)
        print(f"[ERROR] get_all_columns: {e}", flush=True)
        _columns_cache = _columns_cache or []
    return _columns_cache


def get_table_columns(table_name):
    """Get columns for a specific table."""
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "SELECT a.attname FROM pg_catalog.pg_attribute a "
            "JOIN pg_catalog.pg_class c ON a.attrelid = c.oid "
            "JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "
            "WHERE n.nspname = 'public' AND c.relname = %s "
            "AND a.attnum > 0 AND NOT a.attisdropped "
            "ORDER BY a.attname",
            (table_name,)
        )
        columns = [row[0] for row in cur.fetchall()]
        cur.close()
        put_db(conn)
        return set(columns)
    except Exception as e:
        put_db(conn)
        print(f"[ERROR] get_table_columns({table_name}): {e}", flush=True)
        return set()


def _build_condition(field, mode):
    if mode == "exact":
        return f'LOWER("{field}") = LOWER(%s)', None
    if mode == "starts":
        return f'"{field}" ILIKE %s', "prefix"
    return f'"{field}" ILIKE %s', "contains"


def _escape_like(value: str) -> str:
    """Escape LIKE/ILIKE special characters so user input is treated literally."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _audit(action, ip, detail=""):
    _audit_log.append({
        "time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "ip": ip,
        "action": action,
        "detail": detail[:200],
    })
    if len(_audit_log) > MAX_AUDIT:
        _audit_log.pop(0)


def _format_ts(value):
    if not value:
        return "Nog niet gecheckt"
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return value
    return value.strftime("%d-%m-%Y %H:%M:%S")


def get_fetcher_status():
    try:
        resp = requests.get(f"{FETCHER_API_URL}/status", timeout=4)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return {
        "running": False,
        "started_at": None,
        "finished_at": None,
        "progress": "Fetcher onbereikbaar",
        "error": "Fetcher onbereikbaar",
        "files_imported": 0,
        "total_rows": 0,
        "log": [],
    }


def get_portal_stats():
    total_rows = 0
    newest_dataset = "Geen dataset"

    fetch_status = get_fetcher_status()
    if fetch_status.get("running"):
        last_checked = "Nu bezig met checken..."
    else:
        last_checked = _format_ts(fetch_status.get("finished_at"))

    conn = None
    cur = None
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "SELECT COALESCE(SUM(row_count), 0) FROM _file_tracker;"
        )
        total_rows = cur.fetchone()[0] or 0

        cur.execute(
            "SELECT folder_name, table_name FROM _file_tracker "
            "ORDER BY imported_at DESC NULLS LAST LIMIT 1;"
        )
        row = cur.fetchone()
        if row:
            folder_name, table_name = row
            if folder_name and table_name:
                newest_dataset = f"{table_name} ({folder_name})"
            elif table_name:
                newest_dataset = table_name
    except Exception:
        pass
    finally:
        if cur:
            cur.close()
        if conn:
            put_db(conn)

    return {
        "total_rows": total_rows,
        "last_checked": last_checked,
        "newest_dataset": newest_dataset,
    }


def login_required(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        # Track sessie activiteit
        sid = session.get("sid")
        if sid and sid in _active_sessions:
            _active_sessions[sid]["last_seen"] = time.time()
        return f(*args, **kwargs)
    return wrapper


def admin_required(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("is_admin"):
            return redirect(url_for("admin_login"))
        return f(*args, **kwargs)
    return wrapper


@app.after_request
def add_security_headers(response):
    # Verwijder ALLE identificerende headers
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Content-Security-Policy"] = "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, private"
    response.headers["Pragma"] = "no-cache"
    # Verwijder identificeerbare headers
    response.headers.pop("Server", None)
    response.headers.pop("X-Powered-By", None)
    response.headers.pop("Via", None)
    return response


def _check_rate_limit(ip):
    """Return True als IP gelocked is."""
    if ip in _login_attempts:
        attempts, locked_until = _login_attempts[ip]
        if locked_until and time.time() < locked_until:
            return True
        if locked_until and time.time() >= locked_until:
            _login_attempts.pop(ip, None)
    return False


def _record_failed_login(ip):
    attempts, _ = _login_attempts.get(ip, (0, None))
    attempts += 1
    locked_until = time.time() + LOCKOUT_SECONDS if attempts >= MAX_LOGIN_ATTEMPTS else None
    _login_attempts[ip] = (attempts, locked_until)


def _clear_login_attempts(ip):
    _login_attempts.pop(ip, None)


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    ip = request.headers.get("X-Real-IP", request.remote_addr)
    if _check_rate_limit(ip):
        return render_template("login.html", error="Te veel pogingen. Probeer het over 5 minuten opnieuw."), 429
    if request.method == "POST":
        _check_csrf()
        pw = request.form.get("password", "")
        pw_hash = hashlib.sha256(pw.encode()).hexdigest()
        if pw_hash == APP_PASSWORD_HASH:
            _clear_login_attempts(ip)
            session.permanent = True
            session["logged_in"] = True
            sid = secrets.token_hex(16)
            session["sid"] = sid
            _active_sessions[sid] = {
                "ip": ip,
                "user_agent": request.headers.get("User-Agent", "")[:100],
                "login_time": time.time(),
                "last_seen": time.time(),
            }
            _audit("login", ip, "Succesvol ingelogd")
            return redirect(url_for("index"))
        _record_failed_login(ip)
        _audit("login_fail", ip, "Mislukte login poging")
        error = "Verkeerd wachtwoord"
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    sid = session.get("sid")
    if sid:
        _active_sessions.pop(sid, None)
    ip = request.headers.get("X-Real-IP", request.remote_addr)
    _audit("logout", ip)
    session.clear()
    return redirect(url_for("login"))


# ── Admin panel ──────────────────────────────────────────────

@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    error = None
    ip = request.headers.get("X-Real-IP", request.remote_addr)
    if _check_rate_limit(ip):
        return render_template("login.html", error="Te veel pogingen."), 429
    if request.method == "POST":
        _check_csrf()
        pw = request.form.get("password", "")
        pw_hash = hashlib.sha256(pw.encode()).hexdigest()
        if pw_hash == ADMIN_PASSWORD_HASH:
            _clear_login_attempts(ip)
            session.permanent = True
            session["is_admin"] = True
            session["logged_in"] = True
            sid = secrets.token_hex(16)
            session["sid"] = sid
            _active_sessions[sid] = {
                "ip": ip,
                "user_agent": "admin",
                "login_time": time.time(),
                "last_seen": time.time(),
            }
            _audit("admin_login", ip)
            return redirect(url_for("admin_panel"))
        _record_failed_login(ip)
        _audit("admin_login_fail", ip)
        error = "Verkeerd wachtwoord"
    return render_template("login.html", error=error)


@app.route("/admin")
@admin_required
def admin_panel():
    now = time.time()
    # Verwijder verlopen sessies (>1 uur inactief)
    expired = [sid for sid, s in _active_sessions.items() if now - s["last_seen"] > 3600]
    for sid in expired:
        _active_sessions.pop(sid, None)

    sessions = []
    for sid, s in _active_sessions.items():
        sessions.append({
            "sid_short": sid[:8] + "...",
            "ip": s["ip"],
            "user_agent": s["user_agent"][:60],
            "login_time": datetime.fromtimestamp(s["login_time"]).strftime("%H:%M:%S"),
            "last_seen": datetime.fromtimestamp(s["last_seen"]).strftime("%H:%M:%S"),
            "idle": f"{int(now - s['last_seen'])}s",
        })

    return render_template(
        "admin.html",
        sessions=sessions,
        audit_log=list(reversed(_audit_log[-50:])),
        total_sessions=len(_active_sessions),
    )


@app.route("/admin/kick", methods=["POST"])
@admin_required
def admin_kick():
    _check_csrf()
    sid = request.form.get("sid", "")
    # Zoek de volledige sid
    for full_sid in list(_active_sessions.keys()):
        if full_sid.startswith(sid.replace("...", "")):
            ip_kicked = _active_sessions[full_sid]["ip"]
            _active_sessions.pop(full_sid, None)
            ip = request.headers.get("X-Real-IP", request.remote_addr)
            _audit("kick", ip, f"Kicked session van {ip_kicked}")
            break
    return redirect(url_for("admin_panel"))


@app.route("/admin/fetch", methods=["POST"])
@admin_required
def admin_fetch():
    """Start a manual fetch via fetcher container API."""
    _check_csrf()
    ip = request.headers.get("X-Real-IP", request.remote_addr)
    
    # Get optional parameters for direct URL fetch
    data = request.get_json(silent=True) or {}
    file_url = data.get("file_url", "").strip()
    db_name = data.get("db_name", "").strip()
    
    try:
        # Pass parameters to fetcher API if provided
        if file_url and db_name:
            resp = requests.post(
                f"{FETCHER_API_URL}/fetch",
                json={"file_url": file_url, "db_name": db_name},
                timeout=6
            )
            _audit("fetch_direct", ip, f"Direct fetch: {file_url} -> {db_name}")
        else:
            resp = requests.post(f"{FETCHER_API_URL}/fetch", timeout=6)
            _audit("fetch_start", ip, "Automatische fetch gestart")
            
        payload = resp.json()
    except Exception as e:
        return jsonify({"ok": False, "error": f"Fetcher niet bereikbaar: {e}"}), 502

    if resp.status_code == 200 and payload.get("ok"):
        return jsonify({"ok": True, "message": payload.get("message", "Fetch gestart")})

    return jsonify({
        "ok": False,
        "error": payload.get("error", "Fetch kon niet gestart worden"),
    }), (409 if resp.status_code == 409 else 502)


@app.route("/admin/fetch/status")
@admin_required
def admin_fetch_status():
    """Return current fetch status from fetcher container."""
    status = get_fetcher_status()
    return jsonify(status)


@app.route("/admin/tables")
@admin_required
def admin_get_tables():
    """Return list of all data tables with row counts."""
    tables = get_account_tables()
    result = []
    
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        
        for table_name in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cur.fetchone()[0]
                result.append({
                    "name": table_name,
                    "rows": row_count
                })
            except Exception:
                result.append({
                    "name": table_name,
                    "rows": 0
                })
        
        cur.close()
        put_db(conn)
    except Exception:
        put_db(conn)
    
    return jsonify({"tables": result})


@app.route("/")
@login_required
def index():
    all_cols = get_all_columns()
    portal_stats = get_portal_stats()
    return render_template(
        "index.html",
        all_columns=all_cols,
        default_on=DEFAULT_ON,
        searchable_fields=SEARCHABLE_FIELDS,
        portal_stats=portal_stats,
    )


@app.route("/row_detail")
@login_required
def row_detail():
    """Return all non-empty fields for a single row as JSON."""
    table = request.args.get("table", "").strip()
    row_id = request.args.get("id", "").strip()

    # Security: only allow known data tables
    valid_tables = set(get_account_tables())
    if not table or table not in valid_tables:
        return jsonify({"error": "Ongeldige tabel"}), 400
    if not row_id:
        return jsonify({"error": "Geen ID opgegeven"}), 400

    conn = None
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(f'SELECT * FROM "{table}" WHERE "Id" = %s LIMIT 1', (row_id,))
        row = cur.fetchone()
        cur.close()
        put_db(conn)
        if not row:
            return jsonify({"error": "Rij niet gevonden"}), 404
        data = {k: v for k, v in row.items() if v is not None and str(v).strip() != ""}
        return jsonify(data)
    except Exception as e:
        if conn:
            put_db(conn)
        return jsonify({"error": "Fout bij ophalen"}), 500


@app.route("/search")
@login_required
def search():
    all_cols = get_all_columns()
    portal_stats = get_portal_stats()
    valid_cols = set(all_cols)

    query = request.args.get("q", "").strip()
    field = request.args.get("field", "Name")
    match = request.args.get("match", "starts")
    refine_query = request.args.get("refine_q", "").strip()
    refine_field = request.args.get("refine_field", "")
    refine_match = request.args.get("refine_match", "contains")
    try:
        page = max(int(request.args.get("page", 1)), 1)
    except (ValueError, TypeError):
        page = 1
    per_page = 50

    # Always use DEFAULT_ON columns, no user selection
    cols = [c for c in all_cols if c in DEFAULT_ON]

    # Alleen zoeken op toegestane velden
    searchable_col_names = {col for col, _ in SEARCHABLE_FIELDS}
    if field not in searchable_col_names:
        field = "Name"
    if match not in {"contains", "starts", "exact"}:
        match = "contains"

    if refine_query and refine_field not in searchable_col_names:
        refine_field = ""
    if refine_match not in {"contains", "starts", "exact"}:
        refine_match = "contains"

    # Ensure all selected columns are valid
    cols = [c for c in cols if c in valid_cols]
    if not cols:
        cols = ["Name"]

    results = []
    total = 0
    elapsed = 0
    has_next = False
    total_pages = 1

    if query:
        offset = (page - 1) * per_page
        # Always include Id for row-detail modal, even if user didn't select it
        id_in_cols = "Id" in cols
        col_list_cols = cols if id_in_cols else cols + ["Id"]
        col_list = ", ".join(f'"{c}"' for c in col_list_cols)
        tables = get_account_tables()

        if not tables:
            return render_template(
                "results.html",
                results=[], cols=cols, query=query, field=field,
                total=0, page=1, total_pages=1, elapsed=0,
                error="Geen data tabellen gevonden.",
                all_columns=all_cols, default_on=DEFAULT_ON,
                portal_stats=portal_stats,
            )

        conn = None
        try:
            conn = get_db()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            t0 = time.time()
            cur.execute("SET LOCAL max_parallel_workers_per_gather = 0;")
            cur.execute("SET LOCAL statement_timeout = '12000ms';")

            # Dynamisch UNION ALL over alle accounts_* tabellen
            count_parts = []
            count_params = []
            data_parts = []
            data_params = []

            main_cond, main_mode = _build_condition(field, match)
            refine_cond, refine_mode = (None, None)
            if refine_query and refine_field:
                refine_cond, refine_mode = _build_condition(refine_field, refine_match)

            # Kolommen die we nodig hebben (inclusief geslecteerde columns en de search fields)
            required_cols = set(col_list_cols) | {field}
            if refine_field:
                required_cols.add(refine_field)

            for t in tables:
                # Controleer of deze tabel alle benodigde kolommen heeft
                table_cols = get_table_columns(t)
                if not required_cols.issubset(table_cols):
                    # Skip deze tabel als het niet alle kolommen heeft
                    continue

                where_parts = [main_cond]

                if main_mode == "prefix":
                    main_value = f"{_escape_like(query)}%"
                elif main_mode == "contains":
                    main_value = f"%{_escape_like(query)}%"
                else:
                    main_value = query
                params = [main_value]

                if refine_cond:
                    where_parts.append(refine_cond)
                    if refine_mode == "prefix":
                        refine_value = f"{_escape_like(refine_query)}%"
                    elif refine_mode == "contains":
                        refine_value = f"%{_escape_like(refine_query)}%"
                    else:
                        refine_value = refine_query
                    params.append(refine_value)

                where_sql = " AND ".join(where_parts)
                count_parts.append(f'SELECT 1 FROM "{t}" WHERE {where_sql}')
                count_params.extend(params)

                data_parts.append(
                    f"""SELECT {col_list}, '{t}' AS "_bron" FROM "{t}" WHERE {where_sql}"""
                )
                data_params.extend(params)

            count_sql = f"SELECT COUNT(*) AS cnt FROM ({' UNION ALL '.join(count_parts)}) sub"
            cur.execute(count_sql, tuple(count_params))
            total = cur.fetchone()["cnt"]
            total_pages = max((total + per_page - 1) // per_page, 1)
            if page > total_pages:
                page = total_pages
                offset = (page - 1) * per_page

            data_sql = f"{' UNION ALL '.join(data_parts)} ORDER BY \"_bron\", 1 LIMIT %s OFFSET %s"
            cur.execute(data_sql, tuple(data_params) + (per_page, offset))
            results = cur.fetchall()
            has_next = page < total_pages
            elapsed = round(time.time() - t0, 3)

            cur.close()
            put_db(conn)
        except Exception as e:
            put_db(conn)
            print(f"[ERROR] search query failed: {e}", flush=True)
            # Nooit database errors tonen aan gebruiker
            return render_template(
                "results.html",
                results=[], cols=cols, query=query, field=field,
                total=0, page=1, total_pages=1, elapsed=0,
                error="Er is een fout opgetreden bij het zoeken. Probeer het opnieuw.",
                all_columns=all_cols, default_on=DEFAULT_ON,
                searchable_fields=SEARCHABLE_FIELDS,
                match=match,
                refine_query=refine_query,
                refine_field=refine_field,
                refine_match=refine_match,
                selected_cols=cols,
                has_next=False,
                portal_stats=portal_stats,
            )

    display_cols = cols + ["_bron"] if results else cols

    return render_template(
        "results.html",
        results=results,
        cols=display_cols,
        selected_cols=cols,
        query=query,
        field=field,
        match=match,
        refine_query=refine_query,
        refine_field=refine_field,
        refine_match=refine_match,
        total=total,
        page=page,
        total_pages=total_pages,
        has_next=has_next,
        elapsed=elapsed,
        error=None,
        all_columns=all_cols,
        default_on=DEFAULT_ON,
        searchable_fields=SEARCHABLE_FIELDS,
        portal_stats=portal_stats,
    )


@app.route("/healthz")
def healthz():
    """Healthcheck endpoint for Docker / load balancers."""
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        put_db(conn)
        return "ok", 200
    except Exception:
        return "db unreachable", 503


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)

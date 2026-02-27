import os
import time
import hashlib
import secrets
import functools
import logging
import json
from datetime import datetime
from flask import Flask, request, render_template, redirect, url_for, session, abort, make_response
import psycopg2
import psycopg2.extras

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

# Access control: "local" = alleen 127.0.0.1/Docker netwerk, "open" = iedereen
# Kan via admin panel worden aangepast
_access_mode = os.getenv("ACCESS_MODE", "local")  # "local" of "open"

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

# Kolommen die standaard AAN staan in de UI
DEFAULT_ON = {
    "Name", "Id", "Type", "Segment__c", "Phone",
    "vlocity_cmt__BillingEmailAddress__c",
    "BillingStreet", "House_Number__c", "House_Number_Extension__c",
    "BillingPostalCode", "BillingCity", "BillingCountry",
    "vlocity_cmt__Status__c", "Description", "Flash_Message__c",
    "Bank_Account_Number__c", "Bank_Account_Holder_Name__c",
    "Payment_Method__c", "Password__c", "IsActive",
    "Sales_Channel__c", "AccountSource",
    "CreatedDate", "LastModifiedDate",
    "InactivationDate__c", "Brand_Type__c",
    "ParentId", "ParentAccountName__c",
    "Account_Salesforce_ID__c",
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
]

# Cache (met TTL zodat nieuwe tabellen automatisch zichtbaar worden)
_columns_cache = None
_columns_cache_time = 0
_tables_cache = None
_tables_cache_time = 0
CACHE_TTL = 30  # 30 seconden


def get_db():
    return psycopg2.connect(**DB_CONFIG)


def get_account_tables():
    """Discover all data_* or accounts_* tables dynamically."""
    global _tables_cache, _tables_cache_time
    if _tables_cache is not None and time.time() - _tables_cache_time < CACHE_TTL:
        return _tables_cache
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
        conn.close()
    except Exception as e:
        print(f"[ERROR] get_account_tables: {e}", flush=True)
        _tables_cache = _tables_cache or []
    return _tables_cache


def get_all_columns():
    global _columns_cache, _columns_cache_time
    if _columns_cache is not None and time.time() - _columns_cache_time < CACHE_TTL:
        return _columns_cache
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
        conn.close()
    except Exception as e:
        print(f"[ERROR] get_all_columns: {e}", flush=True)
        _columns_cache = _columns_cache or []
    return _columns_cache


def _build_condition(field, mode):
    if mode == "exact":
        return f'LOWER("{field}") = LOWER(%s)', None
    if mode == "starts":
        return f'"{field}" ILIKE %s', "prefix"
    return f'"{field}" ILIKE %s', "contains"


def _audit(action, ip, detail=""):
    _audit_log.append({
        "time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "ip": ip,
        "action": action,
        "detail": detail[:200],
    })
    if len(_audit_log) > MAX_AUDIT:
        _audit_log.pop(0)


def _is_local_ip(ip):
    """Check of IP lokaal is (localhost, Docker netwerk, of privé)."""
    if not ip:
        return False
    return (
        ip.startswith("127.") or
        ip.startswith("172.") or
        ip.startswith("10.") or
        ip.startswith("192.168.") or
        ip == "::1"
    )


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


@app.before_request
def check_access():
    """Blokkeer externe IPs als access_mode op 'local' staat."""
    global _access_mode
    # Login pagina's altijd bereikbaar (anders kun je niet inloggen)
    if request.path in ("/login", "/admin/login"):
        return
    # Static files altijd ok
    if request.path.startswith("/static"):
        return
    # Check access mode
    if _access_mode == "local":
        ip = request.headers.get("X-Real-IP", request.remote_addr)
        if not _is_local_ip(ip):
            abort(403)


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
    global _access_mode
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
        access_mode=_access_mode,
        audit_log=list(reversed(_audit_log[-50:])),
        total_sessions=len(_active_sessions),
    )


@app.route("/admin/access", methods=["POST"])
@admin_required
def admin_access():
    global _access_mode
    mode = request.form.get("mode", "local")
    if mode in ("local", "open"):
        _access_mode = mode
        ip = request.headers.get("X-Real-IP", request.remote_addr)
        _audit("access_change", ip, f"Mode: {mode}")
    return redirect(url_for("admin_panel"))


@app.route("/admin/kick", methods=["POST"])
@admin_required
def admin_kick():
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


@app.route("/")
@login_required
def index():
    all_cols = get_all_columns()
    return render_template(
        "index.html",
        all_columns=all_cols,
        default_on=DEFAULT_ON,
        searchable_fields=SEARCHABLE_FIELDS,
    )


@app.route("/search")
@login_required
def search():
    all_cols = get_all_columns()
    valid_cols = set(all_cols)

    query = request.args.get("q", "").strip()
    field = request.args.get("field", "Name")
    match = request.args.get("match", "starts")
    refine_query = request.args.get("refine_q", "").strip()
    refine_field = request.args.get("refine_field", "")
    refine_match = request.args.get("refine_match", "contains")
    cols = request.args.getlist("cols")
    page = max(int(request.args.get("page", 1)), 1)
    per_page = 50

    if not cols:
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
        col_list = ", ".join(f'"{c}"' for c in cols)
        tables = get_account_tables()

        if not tables:
            return render_template(
                "results.html",
                results=[], cols=cols, query=query, field=field,
                total=0, page=1, total_pages=1, elapsed=0,
                error="Geen data tabellen gevonden.",
                all_columns=all_cols, default_on=DEFAULT_ON,
            )

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

            for t in tables:
                where_parts = [main_cond]

                if main_mode == "prefix":
                    main_value = f"{query}%"
                elif main_mode == "contains":
                    main_value = f"%{query}%"
                else:
                    main_value = query
                params = [main_value]

                if refine_cond:
                    where_parts.append(refine_cond)
                    if refine_mode == "prefix":
                        refine_value = f"{refine_query}%"
                    elif refine_mode == "contains":
                        refine_value = f"%{refine_query}%"
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
            conn.close()
        except Exception as e:
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
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)

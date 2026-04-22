"""
ZCDC Local Vendor Outstanding Payments Tracking System - v2
New features:
- Monthly categorization for vendors/invoices
- EcoCash number field
- Rejections report by month
- Multi-vendor payment batches
- Account numbers visible on dashboard
- Vendor search
- Cost centre as number
- Amount editable on payment page
- User login with role display
"""
import os, sqlite3, uuid
from datetime import date, timedelta
from collections import defaultdict
from flask import Flask, request, jsonify, send_from_directory, session

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, static_folder=os.path.join(BASE_DIR, "static"))
app.secret_key = "zcdc_secret_2024"
DB_PATH = os.path.join(BASE_DIR, "zcdc_vendor_payments.db")

BUILTIN_USERS = {
    "finance_manager_alice": {"password":"admin123",    "role":"Finance Manager",  "initials":"FA"},
    "fin_officer_tom":       {"password":"officer123",  "role":"Finance Officer",  "initials":"FT"},
    "clerk_jane":            {"password":"clerk123",    "role":"Clerk",            "initials":"CJ"},
    "clerk_bob":             {"password":"clerk123",    "role":"Clerk",            "initials":"CB"},
    "treasury_officer_sue":  {"password":"treasury123", "role":"Treasury Officer", "initials":"TS"},
}

VALID_ROLES = ["Finance Manager", "Finance Officer", "Clerk", "Treasury Officer"]

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db():
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            username    TEXT PRIMARY KEY,
            password    TEXT NOT NULL,
            role        TEXT NOT NULL,
            initials    TEXT NOT NULL,
            created_at  TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS vendors (
            vendor_id       TEXT PRIMARY KEY,
            name            TEXT NOT NULL UNIQUE,
            category        TEXT NOT NULL CHECK(category IN ('Services','Supplies','Other')),
            payment_terms   INTEGER DEFAULT 30,
            payment_method  TEXT DEFAULT 'Bank',
            bank_name       TEXT,
            bank_account    TEXT,
            ecocash_number  TEXT,
            created_month   TEXT,
            created_at      TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS invoices (
            invoice_id         TEXT PRIMARY KEY,
            invoice_number     TEXT NOT NULL,
            vendor_id          TEXT NOT NULL REFERENCES vendors(vendor_id),
            invoice_date       TEXT NOT NULL,
            due_date           TEXT,
            description        TEXT NOT NULL,
            total_amount       REAL NOT NULL CHECK(total_amount > 0),
            outstanding_amount REAL NOT NULL,
            cost_centre_number INTEGER,
            cost_centre_name   TEXT,
            doc_reference      TEXT,
            status             TEXT NOT NULL DEFAULT 'Draft'
                               CHECK(status IN ('Draft','Submitted','Verified','Rejected',
                                                'Approved','Scheduled','Partially Paid','Paid')),
            rejection_reason   TEXT,
            created_by         TEXT NOT NULL,
            invoice_month      TEXT,
            created_at         TEXT DEFAULT (datetime('now')),
            UNIQUE(invoice_number, vendor_id)
        );
        CREATE TABLE IF NOT EXISTS workflow_log (
            log_id       TEXT PRIMARY KEY,
            invoice_id   TEXT NOT NULL REFERENCES invoices(invoice_id),
            action       TEXT NOT NULL,
            from_status  TEXT,
            to_status    TEXT,
            performed_by TEXT NOT NULL,
            notes        TEXT,
            performed_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS payment_batches (
            batch_id        TEXT PRIMARY KEY,
            batch_reference TEXT NOT NULL UNIQUE,
            scheduled_date  TEXT NOT NULL,
            notes           TEXT,
            created_by      TEXT NOT NULL,
            created_at      TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS batch_items (
            item_id          TEXT PRIMARY KEY,
            batch_id         TEXT NOT NULL REFERENCES payment_batches(batch_id),
            invoice_id       TEXT NOT NULL REFERENCES invoices(invoice_id),
            scheduled_amount REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS payments (
            payment_id     TEXT PRIMARY KEY,
            invoice_id     TEXT NOT NULL REFERENCES invoices(invoice_id),
            batch_id       TEXT REFERENCES payment_batches(batch_id),
            payment_date   TEXT NOT NULL,
            amount_paid    REAL NOT NULL,
            payment_method TEXT DEFAULT 'Bank',
            bank_reference TEXT,
            recorded_by    TEXT NOT NULL,
            recorded_at    TEXT DEFAULT (datetime('now'))
        );
        """)

def new_id(): return str(uuid.uuid4())

def log_action(conn, invoice_id, action, from_status, to_status, performed_by, notes=""):
    conn.execute(
        "INSERT INTO workflow_log (log_id,invoice_id,action,from_status,to_status,performed_by,notes) VALUES (?,?,?,?,?,?,?)",
        (new_id(), invoice_id, action, from_status, to_status, performed_by, notes)
    )

def age_bucket(d):
    try:
        age = (date.today() - date.fromisoformat(d)).days
        if age<=30: return "0-30 days"
        if age<=60: return "31-60 days"
        if age<=90: return "61-90 days"
        return "91+ days"
    except: return "Unknown"

def month_label(d):
    try: return date.fromisoformat(d[:10]).strftime("%B %Y")
    except: return ""

def rows_to_list(rows): return [dict(r) for r in rows]

# ── AUTH ──────────────────────────────────────────────────────────────────────
def lookup_user(username):
    """Check DB first, then fall back to built-in users."""
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
        if row:
            return {"password": row["password"], "role": row["role"], "initials": row["initials"]}
    return BUILTIN_USERS.get(username)

@app.route("/api/register", methods=["POST"])
def register():
    d = request.json
    username = (d.get("username") or "").strip()
    password = (d.get("password") or "").strip()
    role     = (d.get("role") or "").strip()
    if not username or not password or not role:
        return jsonify({"error": "Username, password and role are required"}), 400
    if role not in VALID_ROLES:
        return jsonify({"error": "Invalid role"}), 400
    if len(username) < 3:
        return jsonify({"error": "Username must be at least 3 characters"}), 400
    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400
    if lookup_user(username):
        return jsonify({"error": "Username already taken"}), 409
    initials = "".join(w[0].upper() for w in username.split("_")[:2])
    with get_conn() as conn:
        conn.execute("INSERT INTO users (username,password,role,initials) VALUES (?,?,?,?)",
                     (username, password, role, initials))
    return jsonify({"ok": True, "username": username, "role": role, "initials": initials})

@app.route("/api/login", methods=["POST"])
def login():
    d = request.json
    username = (d.get("username") or "").strip()
    u = lookup_user(username)
    if u and u["password"] == (d.get("password") or "").strip():
        session["username"] = username
        session["role"]     = u["role"]
        session["initials"] = u["initials"]
        return jsonify({"ok": True, "username": username, "role": u["role"], "initials": u["initials"]})
    return jsonify({"error": "Invalid username or password"}), 401

@app.route("/api/logout", methods=["POST"])
def logout():
    session.clear()
    return jsonify({"ok": True})

@app.route("/api/me")
def me():
    if "username" in session:
        return jsonify({"username": session["username"], "role": session["role"], "initials": session["initials"]})
    return jsonify({"error": "Not logged in"}), 401

@app.route("/api/roles")
def get_roles():
    return jsonify(VALID_ROLES)

# ── VENDORS ───────────────────────────────────────────────────────────────────
@app.route("/api/vendors", methods=["GET"])
def list_vendors():
    search = request.args.get("search","")
    month  = request.args.get("month","")
    sql = "SELECT * FROM vendors WHERE 1=1"
    params = []
    if search:
        sql += " AND (name LIKE ? OR bank_account LIKE ? OR ecocash_number LIKE ?)"
        params += [f"%{search}%"]*3
    if month:
        sql += " AND created_month=?"; params.append(month)
    sql += " ORDER BY name"
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return jsonify(rows_to_list(rows))

@app.route("/api/vendors/months")
def vendor_months():
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT created_month FROM vendors WHERE created_month IS NOT NULL ORDER BY created_month DESC"
        ).fetchall()
    return jsonify([r["created_month"] for r in rows])

@app.route("/api/vendors/<vid>")
def get_vendor(vid):
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM vendors WHERE vendor_id=?", (vid,)).fetchone()
        invs = conn.execute("SELECT * FROM invoices WHERE vendor_id=? ORDER BY invoice_date DESC", (vid,)).fetchall()
    if not row: return jsonify({"error":"Not found"}), 404
    d = dict(row)
    d["invoices"] = rows_to_list(invs)
    d["total_outstanding"] = sum(i["outstanding_amount"] for i in d["invoices"] if i["status"] not in ("Paid","Rejected"))
    return jsonify(d)

@app.route("/api/vendors", methods=["POST"])
def add_vendor():
    d = request.json
    vid = new_id()
    cm = date.today().strftime("%Y-%m")
    try:
        with get_conn() as conn:
            conn.execute(
                "INSERT INTO vendors (vendor_id,name,category,payment_terms,payment_method,bank_name,bank_account,ecocash_number,created_month) VALUES (?,?,?,?,?,?,?,?,?)",
                (vid,d["name"],d["category"],d.get("payment_terms",30),d.get("payment_method","Bank"),
                 d.get("bank_name",""),d.get("bank_account",""),d.get("ecocash_number",""),cm)
            )
        return jsonify({"ok":True,"vendor_id":vid}), 201
    except sqlite3.IntegrityError as e:
        return jsonify({"error":str(e)}), 400

# ── INVOICES ──────────────────────────────────────────────────────────────────
@app.route("/api/invoices", methods=["GET"])
def list_invoices():
    status = request.args.get("status","")
    vendor = request.args.get("vendor_id","")
    month  = request.args.get("month","")
    search = request.args.get("search","")
    sql = """SELECT i.*, v.name AS vendor_name, v.bank_account, v.ecocash_number, v.payment_method AS vendor_pay_method
             FROM invoices i JOIN vendors v ON i.vendor_id=v.vendor_id WHERE 1=1"""
    params = []
    if status: sql += " AND i.status=?"; params.append(status)
    if vendor: sql += " AND i.vendor_id=?"; params.append(vendor)
    if month:  sql += " AND i.invoice_month=?"; params.append(month)
    if search:
        sql += " AND (i.invoice_number LIKE ? OR v.name LIKE ?)"; params += [f"%{search}%"]*2
    sql += " ORDER BY i.invoice_date DESC"
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    result = []
    for r in rows:
        d = dict(r); d["age_bucket"] = age_bucket(d["invoice_date"]); d["month_label"] = month_label(d["invoice_date"])
        result.append(d)
    return jsonify(result)

@app.route("/api/invoices/months")
def invoice_months():
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT invoice_month FROM invoices WHERE invoice_month IS NOT NULL ORDER BY invoice_month DESC"
        ).fetchall()
    return jsonify([r["invoice_month"] for r in rows])

@app.route("/api/invoices", methods=["POST"])
def add_invoice():
    d = request.json
    iid = new_id()
    idate = d["invoice_date"]
    imonth = idate[:7]
    try:
        with get_conn() as conn:
            conn.execute(
                "INSERT INTO invoices (invoice_id,invoice_number,vendor_id,invoice_date,due_date,description,total_amount,outstanding_amount,cost_centre_number,cost_centre_name,doc_reference,created_by,invoice_month) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (iid,d["invoice_number"],d["vendor_id"],idate,d.get("due_date") or None,
                 d["description"],float(d["total_amount"]),float(d["total_amount"]),
                 d.get("cost_centre_number") or None,d.get("cost_centre_name",""),
                 d.get("doc_reference",""),d["created_by"],imonth)
            )
            log_action(conn, iid, "Created", None, "Draft", d["created_by"])
        return jsonify({"ok":True,"invoice_id":iid}), 201
    except sqlite3.IntegrityError as e:
        return jsonify({"error":str(e)}), 400

@app.route("/api/invoices/<iid>")
def get_invoice(iid):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT i.*, v.name AS vendor_name, v.bank_account, v.ecocash_number FROM invoices i JOIN vendors v ON i.vendor_id=v.vendor_id WHERE i.invoice_id=?", (iid,)
        ).fetchone()
        logs = conn.execute("SELECT * FROM workflow_log WHERE invoice_id=? ORDER BY performed_at", (iid,)).fetchall()
    if not row: return jsonify({"error":"Not found"}), 404
    d = dict(row); d["audit"] = rows_to_list(logs); d["age_bucket"] = age_bucket(d["invoice_date"])
    return jsonify(d)

@app.route("/api/invoices/<iid>/workflow", methods=["POST"])
def workflow(iid):
    d = request.json
    action = d.get("action")
    performed_by = d.get("performed_by", session.get("username","system"))
    notes = d.get("notes","")
    transitions = {
        "submit":  ("Draft",                "Submitted"),
        "verify":  ("Submitted",            "Verified"),
        "approve": ("Verified",             "Approved"),
        "reject":  (("Submitted","Verified"),"Rejected"),
    }
    if action not in transitions: return jsonify({"error":"Invalid action"}), 400
    with get_conn() as conn:
        inv = conn.execute("SELECT * FROM invoices WHERE invoice_id=?", (iid,)).fetchone()
        if not inv: return jsonify({"error":"Invoice not found"}), 404
        allowed, to_status = transitions[action]
        if isinstance(allowed, str): allowed = (allowed,)
        if inv["status"] not in allowed:
            return jsonify({"error":f"Cannot {action}: status is '{inv['status']}'"}), 400
        rr = notes if action=="reject" else inv["rejection_reason"]
        conn.execute("UPDATE invoices SET status=?, rejection_reason=? WHERE invoice_id=?", (to_status,rr,iid))
        log_action(conn, iid, action.capitalize(), inv["status"], to_status, performed_by, notes)
    return jsonify({"ok":True,"new_status":to_status})

# ── BATCHES ───────────────────────────────────────────────────────────────────
@app.route("/api/batches", methods=["GET"])
def list_batches():
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT pb.*, COUNT(DISTINCT bi.invoice_id) AS invoice_count,
                      COUNT(DISTINCT i.vendor_id) AS vendor_count,
                      COALESCE(SUM(bi.scheduled_amount),0) AS total_scheduled
               FROM payment_batches pb
               LEFT JOIN batch_items bi ON pb.batch_id=bi.batch_id
               LEFT JOIN invoices i ON bi.invoice_id=i.invoice_id
               GROUP BY pb.batch_id ORDER BY pb.scheduled_date DESC"""
        ).fetchall()
    return jsonify(rows_to_list(rows))

@app.route("/api/batches", methods=["POST"])
def create_batch():
    d = request.json
    bid = new_id()
    created_by = d.get("created_by", session.get("username","system"))
    items = d.get("items", [])  # [{invoice_id, scheduled_amount}, ...]
    try:
        with get_conn() as conn:
            conn.execute(
                "INSERT INTO payment_batches (batch_id,batch_reference,scheduled_date,notes,created_by) VALUES (?,?,?,?,?)",
                (bid, d["batch_reference"], d["scheduled_date"], d.get("notes",""), created_by)
            )
            for item in items:
                iid = item["invoice_id"]
                amount = float(item["scheduled_amount"])
                inv = conn.execute("SELECT * FROM invoices WHERE invoice_id=?", (iid,)).fetchone()
                if not inv:
                    return jsonify({"error": f"Invoice {iid} not found"}), 404
                if inv["status"] != "Approved":
                    return jsonify({"error": f"Invoice {inv['invoice_number']} must be Approved (current: {inv['status']})"}), 400
                if amount > inv["outstanding_amount"]:
                    return jsonify({"error": f"Amount for {inv['invoice_number']} exceeds outstanding balance"}), 400
                conn.execute("INSERT INTO batch_items (item_id,batch_id,invoice_id,scheduled_amount) VALUES (?,?,?,?)",
                             (new_id(), bid, iid, amount))
                conn.execute("UPDATE invoices SET status='Scheduled' WHERE invoice_id=?", (iid,))
                log_action(conn, iid, "Scheduled", "Approved", "Scheduled", created_by, f"Batch {d['batch_reference']}")
        return jsonify({"ok": True, "batch_id": bid}), 201
    except sqlite3.IntegrityError as e:
        return jsonify({"error": str(e)}), 400

@app.route("/api/batches/<bid>/items", methods=["GET"])
def get_batch_items(bid):
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT bi.*, i.invoice_number, i.outstanding_amount, i.status,
                      v.name AS vendor_name, v.bank_account, v.ecocash_number, v.payment_method
               FROM batch_items bi
               JOIN invoices i ON bi.invoice_id=i.invoice_id
               JOIN vendors v ON i.vendor_id=v.vendor_id
               WHERE bi.batch_id=?""", (bid,)
        ).fetchall()
    return jsonify(rows_to_list(rows))

@app.route("/api/batches/<bid>/items", methods=["POST"])
def add_to_batch(bid):
    d = request.json
    iid = d["invoice_id"]; amount = float(d["scheduled_amount"])
    added_by = d.get("added_by", session.get("username","system"))
    with get_conn() as conn:
        inv = conn.execute("SELECT * FROM invoices WHERE invoice_id=?", (iid,)).fetchone()
        if not inv: return jsonify({"error":"Invoice not found"}), 404
        if inv["status"] != "Approved":
            return jsonify({"error":f"Invoice must be Approved (current: {inv['status']})"}), 400
        if amount > inv["outstanding_amount"]:
            return jsonify({"error":"Scheduled amount exceeds outstanding balance"}), 400
        conn.execute("INSERT INTO batch_items (item_id,batch_id,invoice_id,scheduled_amount) VALUES (?,?,?,?)",
                     (new_id(),bid,iid,amount))
        conn.execute("UPDATE invoices SET status='Scheduled' WHERE invoice_id=?", (iid,))
        log_action(conn, iid, "Scheduled", "Approved", "Scheduled", added_by, f"Batch {bid[:8]}")
    return jsonify({"ok":True})

# ── PAYMENTS ──────────────────────────────────────────────────────────────────
@app.route("/api/payments", methods=["POST"])
def record_payment():
    d = request.json
    iid = d["invoice_id"]; amount = float(d["amount_paid"])
    with get_conn() as conn:
        inv = conn.execute("SELECT * FROM invoices WHERE invoice_id=?", (iid,)).fetchone()
        if not inv: return jsonify({"error":"Invoice not found"}), 404
        if inv["status"] not in ("Approved","Scheduled","Partially Paid"):
            return jsonify({"error":f"Cannot pay invoice with status '{inv['status']}'"}), 400
        if amount > inv["outstanding_amount"]:
            return jsonify({"error":"Payment exceeds outstanding balance"}), 400
        new_out = round(inv["outstanding_amount"] - amount, 2)
        new_status = "Paid" if new_out==0 else "Partially Paid"
        conn.execute(
            "INSERT INTO payments (payment_id,invoice_id,batch_id,payment_date,amount_paid,payment_method,bank_reference,recorded_by) VALUES (?,?,?,?,?,?,?,?)",
            (new_id(),iid,d.get("batch_id"),d["payment_date"],amount,
             d.get("payment_method","Bank"),d.get("bank_reference",""),
             d.get("recorded_by",session.get("username","system")))
        )
        conn.execute("UPDATE invoices SET outstanding_amount=?, status=? WHERE invoice_id=?", (new_out,new_status,iid))
        log_action(conn, iid, f"Payment ({new_status})", inv["status"], new_status,
                   d.get("recorded_by","system"),
                   f"Paid:{amount} Method:{d.get('payment_method','Bank')} Ref:{d.get('bank_reference','')}")
    return jsonify({"ok":True,"new_status":new_status,"outstanding":new_out})

# ── REPORTS ───────────────────────────────────────────────────────────────────
@app.route("/api/reports/dashboard")
def dashboard():
    with get_conn() as conn:
        s = {}
        s["total_outstanding"] = conn.execute("SELECT COALESCE(SUM(outstanding_amount),0) AS v FROM invoices WHERE status NOT IN ('Paid','Rejected')").fetchone()["v"]
        s["pending_approval"]  = conn.execute("SELECT COUNT(*) AS v FROM invoices WHERE status IN ('Draft','Submitted','Verified')").fetchone()["v"]
        s["approved_unscheduled"] = conn.execute("SELECT COUNT(*) AS v FROM invoices WHERE status='Approved'").fetchone()["v"]
        s["scheduled_total"]   = conn.execute("SELECT COALESCE(SUM(outstanding_amount),0) AS v FROM invoices WHERE status='Scheduled'").fetchone()["v"]
        s["overdue_91plus"]    = conn.execute("SELECT COUNT(*) AS v FROM invoices WHERE status NOT IN ('Paid','Rejected') AND invoice_date <= date('now','-91 days')").fetchone()["v"]
        s["paid_this_month"]   = conn.execute("SELECT COALESCE(SUM(amount_paid),0) AS v FROM payments WHERE strftime('%Y-%m',payment_date)=strftime('%Y-%m','now')").fetchone()["v"]
        s["total_vendors"]     = conn.execute("SELECT COUNT(*) AS v FROM vendors").fetchone()["v"]
        s["rejected_this_month"] = conn.execute("SELECT COUNT(*) AS v FROM invoices WHERE status='Rejected' AND strftime('%Y-%m',created_at)=strftime('%Y-%m','now')").fetchone()["v"]
        s["top_vendors"] = rows_to_list(conn.execute(
            """SELECT v.name, v.bank_account, v.ecocash_number, v.payment_method,
                      COALESCE(SUM(i.outstanding_amount),0) AS outstanding
               FROM vendors v
               LEFT JOIN invoices i ON v.vendor_id=i.vendor_id AND i.status NOT IN ('Paid','Rejected')
               GROUP BY v.vendor_id ORDER BY outstanding DESC LIMIT 6"""
        ).fetchall())
    return jsonify(s)

@app.route("/api/reports/outstanding")
def report_outstanding():
    month = request.args.get("month","")
    sql = """SELECT v.name AS vendor, v.category, v.bank_account, v.ecocash_number,
                    v.payment_method, COUNT(i.invoice_id) AS invoice_count,
                    COALESCE(SUM(i.outstanding_amount),0) AS total_outstanding
             FROM invoices i JOIN vendors v ON i.vendor_id=v.vendor_id
             WHERE i.status NOT IN ('Paid','Rejected')"""
    params = []
    if month: sql += " AND i.invoice_month=?"; params.append(month)
    sql += " GROUP BY v.vendor_id ORDER BY total_outstanding DESC"
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return jsonify(rows_to_list(rows))

@app.route("/api/reports/aging")
def report_aging():
    month = request.args.get("month","")
    sql = "SELECT v.name AS vendor, i.invoice_date, i.outstanding_amount FROM invoices i JOIN vendors v ON i.vendor_id=v.vendor_id WHERE i.status NOT IN ('Paid','Rejected')"
    params = []
    if month: sql += " AND i.invoice_month=?"; params.append(month)
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    buckets = ["0-30 days","31-60 days","61-90 days","91+ days"]
    vd = defaultdict(lambda: defaultdict(float)); totals = defaultdict(float)
    for r in rows:
        b = age_bucket(r["invoice_date"]); vd[r["vendor"]][b] += r["outstanding_amount"]; totals[b] += r["outstanding_amount"]
    result = []
    for vendor, bdata in vd.items():
        row = {"vendor":vendor,"total":sum(bdata.values())}
        for b in buckets: row[b] = bdata.get(b,0)
        result.append(row)
    result.sort(key=lambda x:-x["total"])
    return jsonify({"vendors":result,"totals":{b:totals.get(b,0) for b in buckets},"grand_total":sum(totals.values())})

@app.route("/api/reports/backlog")
def report_backlog():
    month = request.args.get("month","")
    sql = "SELECT i.*, v.name AS vendor_name FROM invoices i JOIN vendors v ON i.vendor_id=v.vendor_id WHERE i.status IN ('Draft','Submitted','Verified')"
    params = []
    if month: sql += " AND i.invoice_month=?"; params.append(month)
    sql += " ORDER BY i.invoice_date"
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return jsonify(rows_to_list(rows))

@app.route("/api/reports/rejections")
def report_rejections():
    month = request.args.get("month","")
    sql = """SELECT i.invoice_number, i.invoice_date, i.total_amount, i.rejection_reason,
                    i.invoice_month, v.name AS vendor_name,
                    wl.performed_by AS rejected_by, wl.performed_at AS rejected_at
             FROM invoices i
             JOIN vendors v ON i.vendor_id=v.vendor_id
             LEFT JOIN workflow_log wl ON wl.invoice_id=i.invoice_id AND wl.to_status='Rejected'
             WHERE i.status='Rejected'"""
    params = []
    if month: sql += " AND i.invoice_month=?"; params.append(month)
    sql += " ORDER BY i.invoice_month DESC, wl.performed_at DESC"
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    grouped = defaultdict(list)
    for r in rows:
        lbl = month_label(r["invoice_date"]); grouped[lbl].append(dict(r))
    return jsonify({"grouped":dict(grouped),"total":len(rows)})

@app.route("/api/reports/schedule")
def report_schedule():
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT pb.batch_reference, pb.scheduled_date, v.name AS vendor,
                      i.invoice_number, bi.scheduled_amount, i.invoice_id,
                      v.payment_method, v.bank_account, v.ecocash_number
               FROM batch_items bi
               JOIN payment_batches pb ON bi.batch_id=pb.batch_id
               JOIN invoices i ON bi.invoice_id=i.invoice_id
               JOIN vendors v ON i.vendor_id=v.vendor_id
               WHERE i.status='Scheduled'
               ORDER BY pb.scheduled_date, v.name"""
        ).fetchall()
    return jsonify(rows_to_list(rows))

@app.route("/api/reports/monthly_summary")
def monthly_summary():
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT invoice_month,
                      COUNT(*) AS total_invoices,
                      COALESCE(SUM(total_amount),0) AS total_amount,
                      COALESCE(SUM(CASE WHEN status='Paid' THEN total_amount ELSE 0 END),0) AS paid_amount,
                      SUM(CASE WHEN status='Rejected' THEN 1 ELSE 0 END) AS rejected_count,
                      COALESCE(SUM(outstanding_amount),0) AS outstanding_amount
               FROM invoices WHERE invoice_month IS NOT NULL
               GROUP BY invoice_month ORDER BY invoice_month DESC"""
        ).fetchall()
    return jsonify(rows_to_list(rows))

# ── FINANCIAL ENGINEERING ANALYTICS ──────────────────────────────────────────

def compute_risk_score(age_days, outstanding, payment_terms, rejection_count, partial_count):
    """
    Vendor Risk Score (0-100). Higher = more risk.
    Factors: age of oldest unpaid invoice, amount outstanding,
    breach of payment terms, rejection history, partial payment history.
    """
    score = 0
    # Age factor (0-40 pts)
    if age_days > 90:   score += 40
    elif age_days > 60: score += 28
    elif age_days > 30: score += 15
    else:               score += 5
    # Payment terms breach (0-20 pts)
    breach = max(0, age_days - payment_terms)
    if breach > 60:   score += 20
    elif breach > 30: score += 13
    elif breach > 0:  score += 7
    # Outstanding amount factor (0-20 pts) — relative to 50k baseline
    amt_score = min(20, int(outstanding / 50000 * 20))
    score += amt_score
    # Rejection history (0-10 pts)
    score += min(10, rejection_count * 3)
    # Partial payment history (0-10 pts)
    score += min(10, partial_count * 5)
    return min(100, score)

def risk_label(score):
    if score >= 70: return "Critical"
    if score >= 50: return "High"
    if score >= 30: return "Medium"
    return "Low"

def risk_color(score):
    if score >= 70: return "critical"
    if score >= 50: return "high"
    if score >= 30: return "medium"
    return "low"

@app.route("/api/analytics/vendor_risk")
def vendor_risk():
    """Vendor risk scorecard — aging, breach, amount, rejections."""
    with get_conn() as conn:
        vendors = conn.execute("SELECT * FROM vendors").fetchall()
        results = []
        for v in vendors:
            vid = v["vendor_id"]
            # Oldest unpaid invoice age
            oldest = conn.execute(
                "SELECT MIN(invoice_date) AS oldest FROM invoices WHERE vendor_id=? AND status NOT IN ('Paid','Rejected')", (vid,)
            ).fetchone()["oldest"]
            age_days = (date.today() - date.fromisoformat(oldest)).days if oldest else 0
            # Total outstanding
            outstanding = conn.execute(
                "SELECT COALESCE(SUM(outstanding_amount),0) AS v FROM invoices WHERE vendor_id=? AND status NOT IN ('Paid','Rejected')", (vid,)
            ).fetchone()["v"]
            # Rejection count
            rej = conn.execute(
                "SELECT COUNT(*) AS v FROM invoices WHERE vendor_id=? AND status='Rejected'", (vid,)
            ).fetchone()["v"]
            # Partial count
            partial = conn.execute(
                "SELECT COUNT(*) AS v FROM invoices WHERE vendor_id=? AND status='Partially Paid'", (vid,)
            ).fetchone()["v"]
            # Invoice count unpaid
            inv_count = conn.execute(
                "SELECT COUNT(*) AS v FROM invoices WHERE vendor_id=? AND status NOT IN ('Paid','Rejected')", (vid,)
            ).fetchone()["v"]
            score = compute_risk_score(age_days, outstanding, v["payment_terms"], rej, partial)
            results.append({
                "vendor_id": vid,
                "vendor": v["name"],
                "category": v["category"],
                "payment_terms": v["payment_terms"],
                "oldest_invoice_age_days": age_days,
                "outstanding": outstanding,
                "open_invoices": inv_count,
                "rejection_count": rej,
                "partial_count": partial,
                "risk_score": score,
                "risk_label": risk_label(score),
                "risk_color": risk_color(score),
            })
        results.sort(key=lambda x: -x["risk_score"])
    return jsonify(results)

@app.route("/api/analytics/cash_projection")
def cash_projection():
    """
    Cash requirement projection for next 90 days.
    Looks at: scheduled batches, approved-unscheduled invoices (estimated by due/terms),
    and partially paid invoices still outstanding.
    Returns week-by-week buckets.
    """
    today = date.today()
    weeks = []
    for w in range(13):  # 13 weeks = ~90 days
        wstart = today + timedelta(days=w*7)
        wend   = wstart + timedelta(days=6)
        weeks.append({"week": w+1, "start": wstart.isoformat(), "end": wend.isoformat(),
                      "scheduled": 0.0, "projected": 0.0, "label": wstart.strftime("%b %d")})

    with get_conn() as conn:
        # Scheduled batches — firm commitments
        sched = conn.execute(
            """SELECT pb.scheduled_date, COALESCE(SUM(bi.scheduled_amount),0) AS amt
               FROM batch_items bi
               JOIN payment_batches pb ON bi.batch_id=pb.batch_id
               JOIN invoices i ON bi.invoice_id=i.invoice_id
               WHERE i.status='Scheduled' AND pb.scheduled_date >= ?
               GROUP BY pb.scheduled_date""",
            (today.isoformat(),)
        ).fetchall()
        for s in sched:
            sdate = date.fromisoformat(s["scheduled_date"])
            for w in weeks:
                if w["start"] <= s["scheduled_date"] <= w["end"]:
                    w["scheduled"] += s["amt"]
                    break

        # Approved (unscheduled) — project by due_date or invoice_date + payment_terms
        approved = conn.execute(
            """SELECT i.invoice_date, i.due_date, i.outstanding_amount, v.payment_terms
               FROM invoices i JOIN vendors v ON i.vendor_id=v.vendor_id
               WHERE i.status='Approved'"""
        ).fetchall()
        for a in approved:
            if a["due_date"]:
                proj_date = date.fromisoformat(a["due_date"])
            else:
                proj_date = date.fromisoformat(a["invoice_date"]) + timedelta(days=a["payment_terms"])
            for w in weeks:
                if w["start"] <= proj_date.isoformat() <= w["end"]:
                    w["projected"] += a["outstanding_amount"]
                    break
                elif proj_date < date.fromisoformat(w["start"]):
                    # Overdue — add to first week
                    weeks[0]["projected"] += a["outstanding_amount"]
                    break

        # Partially paid — add remaining to nearest projected week
        partial = conn.execute(
            """SELECT i.invoice_date, i.outstanding_amount, v.payment_terms
               FROM invoices i JOIN vendors v ON i.vendor_id=v.vendor_id
               WHERE i.status='Partially Paid'"""
        ).fetchall()
        for p in partial:
            proj_date = date.fromisoformat(p["invoice_date"]) + timedelta(days=p["payment_terms"])
            for w in weeks:
                if w["start"] <= proj_date.isoformat() <= w["end"]:
                    w["projected"] += p["outstanding_amount"]
                    break
                elif proj_date < date.fromisoformat(w["start"]):
                    weeks[0]["projected"] += p["outstanding_amount"]
                    break

    # Cumulative
    cum = 0
    for w in weeks:
        w["total"] = round(w["scheduled"] + w["projected"], 2)
        cum += w["total"]
        w["cumulative"] = round(cum, 2)
        w["scheduled"] = round(w["scheduled"], 2)
        w["projected"] = round(w["projected"], 2)

    total_90 = sum(w["total"] for w in weeks)
    return jsonify({"weeks": weeks, "total_90_days": round(total_90, 2)})

@app.route("/api/analytics/working_capital")
def working_capital():
    """
    Working capital / AP analytics:
    - DPO (Days Payable Outstanding)
    - AP Turnover
    - Payment velocity trend (monthly paid vs invoiced)
    - Approval cycle time (avg days Draft→Approved)
    """
    with get_conn() as conn:
        # Total AP (outstanding)
        total_ap = conn.execute(
            "SELECT COALESCE(SUM(outstanding_amount),0) AS v FROM invoices WHERE status NOT IN ('Paid','Rejected')"
        ).fetchone()["v"]
        # Total purchases (all invoices excl rejected)
        total_purchases = conn.execute(
            "SELECT COALESCE(SUM(total_amount),0) AS v FROM invoices WHERE status != 'Rejected'"
        ).fetchone()["v"]
        # Total paid
        total_paid = conn.execute(
            "SELECT COALESCE(SUM(amount_paid),0) AS v FROM payments"
        ).fetchone()["v"]
        # DPO = (AP / COGS) * Days — approximate with 365 days
        dpo = round((total_ap / total_purchases * 365), 1) if total_purchases > 0 else 0
        # AP Turnover = Total Purchases / Average AP (approximate with current AP)
        ap_turnover = round(total_purchases / total_ap, 2) if total_ap > 0 else 0
        # Approval cycle time: avg days from created_at to Approved log entry
        cycle_rows = conn.execute(
            """SELECT i.invoice_id,
                      julianday(wl.performed_at) - julianday(i.created_at) AS cycle_days
               FROM invoices i
               JOIN workflow_log wl ON wl.invoice_id=i.invoice_id AND wl.to_status='Approved'"""
        ).fetchall()
        avg_cycle = round(sum(r["cycle_days"] for r in cycle_rows) / len(cycle_rows), 1) if cycle_rows else 0
        # Monthly payment velocity
        velocity = conn.execute(
            """SELECT strftime('%Y-%m', payment_date) AS month,
                      COALESCE(SUM(amount_paid),0) AS paid
               FROM payments GROUP BY month ORDER BY month DESC LIMIT 6"""
        ).fetchall()
        # Monthly invoiced
        invoiced = conn.execute(
            """SELECT invoice_month AS month, COALESCE(SUM(total_amount),0) AS invoiced
               FROM invoices WHERE invoice_month IS NOT NULL
               GROUP BY invoice_month ORDER BY invoice_month DESC LIMIT 6"""
        ).fetchall()
        vel_map = {r["month"]: r["paid"] for r in velocity}
        inv_map = {r["month"]: r["invoiced"] for r in invoiced}
        all_months = sorted(set(list(vel_map.keys())+list(inv_map.keys())), reverse=True)[:6]
        trend = [{"month": m, "paid": vel_map.get(m,0), "invoiced": inv_map.get(m,0)} for m in reversed(all_months)]

    return jsonify({
        "total_ap": total_ap,
        "total_purchases": total_purchases,
        "total_paid": total_paid,
        "dpo": dpo,
        "ap_turnover": ap_turnover,
        "avg_approval_cycle_days": avg_cycle,
        "payment_velocity_trend": trend,
    })

@app.route("/api/analytics/aging_trend")
def aging_trend():
    """Monthly aging snapshot — how the aging buckets evolved over time."""
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT invoice_month,
                      COALESCE(SUM(CASE WHEN julianday('now')-julianday(invoice_date)<=30 THEN outstanding_amount ELSE 0 END),0) AS b0_30,
                      COALESCE(SUM(CASE WHEN julianday('now')-julianday(invoice_date) BETWEEN 31 AND 60 THEN outstanding_amount ELSE 0 END),0) AS b31_60,
                      COALESCE(SUM(CASE WHEN julianday('now')-julianday(invoice_date) BETWEEN 61 AND 90 THEN outstanding_amount ELSE 0 END),0) AS b61_90,
                      COALESCE(SUM(CASE WHEN julianday('now')-julianday(invoice_date)>90 THEN outstanding_amount ELSE 0 END),0) AS b91plus
               FROM invoices
               WHERE status NOT IN ('Paid','Rejected') AND invoice_month IS NOT NULL
               GROUP BY invoice_month ORDER BY invoice_month"""
        ).fetchall()
    return jsonify(rows_to_list(rows))

@app.route("/api/analytics/cost_centre_breakdown")
def cost_centre_breakdown():
    """Outstanding by cost centre — supports budget control analysis."""
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT cost_centre_number, cost_centre_name,
                      COUNT(*) AS invoice_count,
                      COALESCE(SUM(outstanding_amount),0) AS outstanding,
                      COALESCE(SUM(total_amount),0) AS total_invoiced
               FROM invoices
               WHERE status NOT IN ('Paid','Rejected') AND cost_centre_number IS NOT NULL
               GROUP BY cost_centre_number ORDER BY outstanding DESC"""
        ).fetchall()
    return jsonify(rows_to_list(rows))

@app.route("/api/analytics/payment_performance")
def payment_performance():
    """
    On-time vs late payment analysis.
    Compares payment_date vs (invoice_date + payment_terms).
    """
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT p.payment_date, i.invoice_date, v.payment_terms, v.name AS vendor,
                      p.amount_paid, i.invoice_number
               FROM payments p
               JOIN invoices i ON p.invoice_id=i.invoice_id
               JOIN vendors v ON i.vendor_id=v.vendor_id"""
        ).fetchall()
    on_time = 0; late = 0; total_delay = 0; vendor_late = defaultdict(int)
    details = []
    for r in rows:
        try:
            pay_date = date.fromisoformat(r["payment_date"])
            due_date = date.fromisoformat(r["invoice_date"]) + timedelta(days=r["payment_terms"])
            delay = (pay_date - due_date).days
            if delay <= 0:
                on_time += 1; status = "On Time"
            else:
                late += 1; total_delay += delay; vendor_late[r["vendor"]] += 1; status = "Late"
            details.append({"vendor": r["vendor"], "invoice": r["invoice_number"],
                            "delay_days": delay, "status": status, "amount": r["amount_paid"]})
        except: pass
    total = on_time + late
    avg_delay = round(total_delay / late, 1) if late > 0 else 0
    return jsonify({
        "on_time": on_time, "late": late, "total": total,
        "on_time_pct": round(on_time/total*100, 1) if total else 0,
        "avg_delay_days": avg_delay,
        "worst_vendors": sorted([{"vendor":k,"late_count":v} for k,v in vendor_late.items()], key=lambda x:-x["late_count"])[:5],
        "details": details[:50],
    })

@app.route("/")
def index():
    return send_from_directory(os.path.join(BASE_DIR,"static"), "index.html")

# ── INIT DB & SEED ────────────────────────────────────────────────────────────
init_db()
with get_conn() as conn:
    if not conn.execute("SELECT 1 FROM vendors LIMIT 1").fetchone():
        today = date.today()
        v1=new_id(); v2=new_id(); v3=new_id()
        m0=today.strftime("%Y-%m")
        m1=(today-timedelta(days=32)).strftime("%Y-%m")
        m2=(today-timedelta(days=62)).strftime("%Y-%m")
        conn.execute("INSERT INTO vendors VALUES (?,?,?,?,?,?,?,?,?,datetime('now'))",
                     (v1,"ABC Electrical Supplies","Supplies",30,"Bank","CABS","ACC-1234567","",m1))
        conn.execute("INSERT INTO vendors VALUES (?,?,?,?,?,?,?,?,?,datetime('now'))",
                     (v2,"XYZ Maintenance Services","Services",45,"EcoCash","","","0771234567",m0))
        conn.execute("INSERT INTO vendors VALUES (?,?,?,?,?,?,?,?,?,datetime('now'))",
                     (v3,"Rapid Logistics Ltd","Services",14,"Bank","ZB","ACC-7654321","",m2))
        def make_inv(no,vid,days,desc,amt,st,ccn,ccname):
            iid=new_id(); idate=(today-timedelta(days=days)).isoformat(); im=idate[:7]
            out=amt if st!="Paid" else 0
            if st=="Partially Paid": out=round(amt*0.4,2)
            conn.execute("INSERT INTO invoices (invoice_id,invoice_number,vendor_id,invoice_date,description,total_amount,outstanding_amount,cost_centre_number,cost_centre_name,status,created_by,invoice_month) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                         (iid,no,vid,idate,desc,amt,out,ccn,ccname,st,"clerk_jane",im))
            conn.execute("INSERT INTO workflow_log VALUES (?,?,?,?,?,?,?,datetime('now'))",
                         (new_id(),iid,"Created",None,"Draft","clerk_jane",""))
        make_inv("INV-2024-001",v1,98,"Electrical cables and fittings",15000,"Approved",1001,"Mining-Ops")
        make_inv("INV-2024-002",v2,50,"Monthly maintenance contract",8500,"Verified",2001,"Plant-Maint")
        make_inv("INV-2024-003",v3,10,"Fuel delivery transport",3200,"Submitted",3001,"Logistics")
        make_inv("INV-2024-004",v1,70,"Copper wire batch B",22000,"Partially Paid",1001,"Mining-Ops")
        make_inv("INV-2024-005",v2,5,"Office cleaning services",1200,"Draft",4001,"Admin")
        make_inv("INV-2024-006",v3,35,"Cold storage logistics",4800,"Approved",3001,"Logistics")

if __name__ == "__main__":
    app.run(debug=False, port=5000)

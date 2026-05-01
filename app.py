"""
ZCDC Vendor Tracking System — v3 (with real email notifications)
=================================================================
Run:  python app.py
Open: http://localhost:5000

Email is sent at every workflow step. Configure email_service.py first.
"""
import os, sqlite3, uuid, threading
from datetime import date, timedelta
from collections import defaultdict
from flask import Flask, request, jsonify, send_from_directory, session

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, static_folder=os.path.join(BASE_DIR, "static"))
app.secret_key = "zcdc_secret_2024"
DB_PATH = os.path.join(BASE_DIR, "zcdc_vendor_payments.db")

# ── Import email service (safe — won't crash if file missing) ─────────────────
try:
    from email_service import Emails, get_email_log, EMAIL_ENABLED
    EMAIL_READY = True
except ImportError:
    EMAIL_READY = False
    EMAIL_ENABLED = False
    class Emails:
        """Stub so app still works without email_service.py"""
        @staticmethod
        def invoice_submitted(*a, **k): pass
        @staticmethod
        def invoice_verified(*a, **k): pass
        @staticmethod
        def invoice_approved(*a, **k): pass
        @staticmethod
        def invoice_rejected(*a, **k): pass
        @staticmethod
        def batch_created(*a, **k): pass
        @staticmethod
        def payment_confirmed(*a, **k): pass
        @staticmethod
        def overdue_alert(*a, **k): pass
        @staticmethod
        def vendor_added(*a, **k): pass
    def get_email_log(limit=100): return []

def _email_async(fn, *args, **kwargs):
    """Run email sending in a background thread so it never slows down the API."""
    threading.Thread(target=fn, args=args, kwargs=kwargs, daemon=True).start()

# ── Built-in demo users ───────────────────────────────────────────────────────
BUILTIN_USERS = {
    "fm_alice":      {"password":"admin123",   "role":"Finance Manager",                         "initials":"FA"},
    "cfo_james":     {"password":"cfo123",     "role":"Chief Finance Officer",                   "initials":"CJ"},
    "afm_bob":       {"password":"afm123",     "role":"Assistant Finance Manager",               "initials":"AB"},
    "cma_zed":       {"password":"cma123",     "role":"Cost and Management Accountant",          "initials":"CZ"},
    "acma_sarah":    {"password":"acma123",    "role":"Assistant Cost and Management Accountant","initials":"AS"},
    "clerk_jane":    {"password":"clerk123",   "role":"Cost and Management Clerk",               "initials":"CL"},
    "rec_tom":       {"password":"recv123",    "role":"Receiving Clerk",                         "initials":"RT"},
    "treasury_sue":  {"password":"treasury123","role":"Treasury Officer",                        "initials":"TS"},
}

VALID_ROLES = [
    "Chief Finance Officer", "Finance Manager", "Assistant Finance Manager",
    "Cost and Management Accountant", "Assistant Cost and Management Accountant",
    "Cost and Management Clerk", "Receiving Clerk", "Treasury Officer",
]

# ── Role permissions (per IMS-FIN-SOP-01 & SOP-02) ───────────────────────────
ROLE_PERMISSIONS = {
    "Receiving Clerk": {
        "can_add_vendor":False,"can_create_invoice":True,"can_submit":True,
        "can_verify":False,"can_approve":False,"can_reject":False,
        "can_create_batch":False,"can_record_payment":False,
        "nav_items":["invoices"],
        "label":"Receives invoices & GRVs. Captures into register. Submits for 3-way match.",
    },
    "Cost and Management Clerk": {
        "can_add_vendor":False,"can_create_invoice":True,"can_submit":True,
        "can_verify":False,"can_approve":False,"can_reject":False,
        "can_create_batch":False,"can_record_payment":False,
        "nav_items":["invoices","vendors","outstanding","aging"],
        "label":"Performs 3-way match (Invoice/PO/GRV). Parks invoices. Requests credit notes.",
    },
    "Assistant Cost and Management Accountant": {
        "can_add_vendor":False,"can_create_invoice":False,"can_submit":False,
        "can_verify":True,"can_approve":False,"can_reject":True,
        "can_create_batch":True,"can_record_payment":False,
        "nav_items":["invoices","batches","outstanding","aging","backlog","rejections","schedule","monthly"],
        "label":"Reviews parked invoices. Prepares creditor reconciliations. Generates payment run.",
    },
    "Cost and Management Accountant": {
        "can_add_vendor":False,"can_create_invoice":False,"can_submit":False,
        "can_verify":True,"can_approve":False,"can_reject":True,
        "can_create_batch":True,"can_record_payment":False,
        "nav_items":["invoices","batches","vendors","outstanding","aging","backlog","rejections","schedule","monthly","risk","wc","performance"],
        "label":"Reviews & posts invoices. Oversees 3-way match. Period-end reconciliations.",
    },
    "Assistant Finance Manager": {
        "can_add_vendor":False,"can_create_invoice":False,"can_submit":False,
        "can_verify":False,"can_approve":False,"can_reject":True,
        "can_create_batch":False,"can_record_payment":False,
        "nav_items":["outstanding","aging","backlog","rejections","schedule","monthly","risk","wc"],
        "label":"Reviews & signs creditor reconciliations. Approves payment vouchers. Reviews reversals.",
    },
    "Treasury Officer": {
        "can_add_vendor":False,"can_create_invoice":False,"can_submit":False,
        "can_verify":False,"can_approve":False,"can_reject":False,
        "can_create_batch":True,"can_record_payment":True,
        "nav_items":["batches","schedule","outstanding","aging","monthly"],
        "label":"Creates payment batches. Records payments. Uploads to Paynet. Executes payment runs.",
    },
    "Finance Manager": {
        "can_add_vendor":True,"can_create_invoice":False,"can_submit":False,
        "can_verify":False,"can_approve":True,"can_reject":True,
        "can_create_batch":False,"can_record_payment":False,
        "nav_items":["invoices","vendors","outstanding","aging","backlog","rejections","schedule","monthly","risk","cashflow","wc","performance"],
        "label":"Approves invoices. Reviews & signs off reconciliations. Authorises payments.",
    },
    "Chief Finance Officer": {
        "can_add_vendor":True,"can_create_invoice":False,"can_submit":False,
        "can_verify":False,"can_approve":True,"can_reject":True,
        "can_create_batch":False,"can_record_payment":False,
        "nav_items":["invoices","vendors","outstanding","aging","backlog","rejections","schedule","monthly","risk","cashflow","wc","performance"],
        "label":"Overall compliance authority. Final sign-off on all financial reports and payments.",
    },
}

def get_perm(role):
    return ROLE_PERMISSIONS.get(role, {
        "can_add_vendor":False,"can_create_invoice":False,"can_submit":False,
        "can_verify":False,"can_approve":False,"can_reject":False,
        "can_create_batch":False,"can_record_payment":False,
        "nav_items":["dashboard"],"label":"Read-only access.",
    })

def check_perm(perm_key):
    role = session.get("role","")
    if not get_perm(role).get(perm_key, False):
        return jsonify({"error": f"Access denied. Your role ({role}) cannot perform this action per ZCDC IMS-FIN-SOP-01/02."}), 403
    return None

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db():
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            username   TEXT PRIMARY KEY,
            password   TEXT NOT NULL,
            role       TEXT NOT NULL,
            initials   TEXT NOT NULL,
            email      TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS vendors (
            vendor_id      TEXT PRIMARY KEY,
            name           TEXT NOT NULL UNIQUE,
            category       TEXT NOT NULL CHECK(category IN ('Services','Supplies','Other')),
            payment_terms  INTEGER DEFAULT 30,
            payment_method TEXT DEFAULT 'Bank',
            bank_name      TEXT,
            bank_account   TEXT,
            ecocash_number TEXT,
            email          TEXT DEFAULT '',
            supplier_type  TEXT DEFAULT 'LOCAL',
            created_month  TEXT,
            created_at     TEXT DEFAULT (datetime('now'))
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
            currency           TEXT DEFAULT 'USD',
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
        CREATE TABLE IF NOT EXISTS email_log (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            status   TEXT,
            to_email TEXT,
            subject  TEXT,
            detail   TEXT,
            sent_at  TEXT DEFAULT (datetime('now'))
        );
        """)
        # Add email column to existing tables if upgrading from older version
        for tbl, col, dflt in [
            ("vendors", "email", "''"),
            ("vendors", "supplier_type", "'LOCAL'"),
            ("users",   "email", "''"),
        ]:
            try:
                conn.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} TEXT DEFAULT {dflt}")
            except Exception:
                pass
        # Seed demo data
        if conn.execute("SELECT COUNT(*) FROM vendors").fetchone()[0] == 0:
            _seed(conn)

def _seed(conn):
    today = date.today()
    m0 = today.strftime("%Y-%m")
    m1 = (today - timedelta(days=35)).strftime("%Y-%m")
    m2 = (today - timedelta(days=65)).strftime("%Y-%m")
    v1, v2, v3 = new_id(), new_id(), new_id()
    conn.execute(
        "INSERT INTO vendors (vendor_id,name,category,payment_terms,payment_method,bank_name,bank_account,ecocash_number,email,supplier_type,created_month) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (v1,"ABC Electrical Supplies","Supplies",30,"Bank","CABS","ACC-1234567","","accounts@abcelectrical.co.zw","LOCAL",m1)
    )
    conn.execute(
        "INSERT INTO vendors (vendor_id,name,category,payment_terms,payment_method,bank_name,bank_account,ecocash_number,email,supplier_type,created_month) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (v2,"XYZ Maintenance Services","Services",45,"EcoCash","","","0771234567","payments@xyzmaint.co.zw","ZCDC",m0)
    )
    conn.execute(
        "INSERT INTO vendors (vendor_id,name,category,payment_terms,payment_method,bank_name,bank_account,ecocash_number,email,supplier_type,created_month) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (v3,"Rapid Logistics Ltd","Services",14,"Bank","ZB","ACC-7654321","","finance@rapidlogistics.co.zw","LOCAL",m2)
    )
    def inv(number, vid, idate, desc, amount, status, cc_num=None, cc_name="", curr="USD"):
        iid = new_id()
        conn.execute(
            "INSERT INTO invoices (invoice_id,invoice_number,vendor_id,invoice_date,description,total_amount,outstanding_amount,currency,cost_centre_number,cost_centre_name,status,created_by,invoice_month) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (iid,number,vid,idate,desc,amount,amount if status not in ("Paid",) else 0,curr,cc_num,cc_name,status,"system",idate[:7])
        )
        conn.execute("INSERT INTO workflow_log (log_id,invoice_id,action,from_status,to_status,performed_by) VALUES (?,?,?,?,?,?)",
                     (new_id(),iid,"Created",None,"Draft","system"))
        if status in ("Submitted","Verified","Approved","Scheduled","Paid"):
            conn.execute("INSERT INTO workflow_log (log_id,invoice_id,action,from_status,to_status,performed_by) VALUES (?,?,?,?,?,?)",
                         (new_id(),iid,"Submit","Draft","Submitted","rec_tom"))
        if status in ("Verified","Approved","Scheduled","Paid"):
            conn.execute("INSERT INTO workflow_log (log_id,invoice_id,action,from_status,to_status,performed_by) VALUES (?,?,?,?,?,?)",
                         (new_id(),iid,"Verify","Submitted","Verified","acma_sarah"))
        if status in ("Approved","Scheduled","Paid"):
            conn.execute("INSERT INTO workflow_log (log_id,invoice_id,action,from_status,to_status,performed_by) VALUES (?,?,?,?,?,?)",
                         (new_id(),iid,"Approve","Verified","Approved","fm_alice"))
        return iid

    d1 = (today - timedelta(days=5)).isoformat()
    d2 = (today - timedelta(days=40)).isoformat()
    d3 = (today - timedelta(days=70)).isoformat()
    d4 = (today - timedelta(days=3)).isoformat()
    d5 = (today - timedelta(days=95)).isoformat()
    inv("INV-001",v1,d1,"Electrical components for pump house",4500.00,"Approved",1001,"Mining-Ops")
    inv("INV-002",v2,d2,"Monthly maintenance contract Q1",12000.00,"Approved",1002,"Maintenance")
    inv("INV-003",v3,d3,"Freight — ore concentrate shipment",3200.00,"Submitted",1003,"Logistics")
    inv("INV-004",v1,d4,"Switchgear replacement",8750.00,"Draft",1001,"Mining-Ops")
    inv("INV-005",v2,d5,"Security services March",5600.00,"Approved",1004,"Security","ZiG")
    inv("INV-006",v3,(today-timedelta(days=15)).isoformat(),"Transport fuel allowance",1800.00,"Verified",1003,"Logistics")

new_id   = lambda: str(uuid.uuid4())
rows_to_list = lambda rows: [dict(r) for r in rows]

def log_action(conn, invoice_id, action, from_status, to_status, performed_by, notes=""):
    conn.execute(
        "INSERT INTO workflow_log (log_id,invoice_id,action,from_status,to_status,performed_by,notes) VALUES (?,?,?,?,?,?,?)",
        (new_id(), invoice_id, action, from_status, to_status, performed_by, notes)
    )

def age_bucket(d):
    if not d: return "Unknown"
    try:
        age = (date.today() - date.fromisoformat(str(d)[:10])).days
        if age <= 30:  return "0-30 days"
        if age <= 60:  return "31-60 days"
        if age <= 90:  return "61-90 days"
        return "91+ days"
    except Exception:
        return "Unknown"

def month_label(d):
    try:
        return date.fromisoformat(str(d)[:10]).strftime("%B %Y")
    except Exception:
        return str(d)[:7] if d else ""

def lookup_user(username):
    if username in BUILTIN_USERS:
        return BUILTIN_USERS[username]
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
        return dict(row) if row else None

def get_vendor_with_email(vendor_id, conn):
    """Fetch vendor including email address."""
    row = conn.execute("SELECT * FROM vendors WHERE vendor_id=?", (vendor_id,)).fetchone()
    return dict(row) if row else {}

# ══════════════════════════════════════════════════════════════════════════════
# AUTH
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/register", methods=["POST"])
def register():
    """
    Registration uses MINE NUMBER as username and 4-digit PIN as password.
    Stored internally as: username=mine_number, password=PIN (4 digits)
    Legacy accounts use regular username/password (min 6 chars).
    """
    d           = request.json
    mine_number = (d.get("mine_number") or d.get("username") or "").strip().upper()
    pin         = (d.get("pin") or d.get("password") or "").strip()
    role        = (d.get("role") or "").strip()
    email       = (d.get("email") or "").strip()
    full_name   = (d.get("full_name") or "").strip()

    if not mine_number or not pin or not role:
        return jsonify({"error": "Mine number, PIN and role are required"}), 400
    if role not in VALID_ROLES:
        return jsonify({"error": "Invalid role"}), 400
    if len(mine_number) < 3:
        return jsonify({"error": "Mine number must be at least 3 characters"}), 400
    if not (len(pin) == 4 and pin.isdigit()):
        return jsonify({"error": "PIN must be exactly 4 digits (e.g. 1234)"}), 400
    if lookup_user(mine_number):
        return jsonify({"error": "Mine number already registered"}), 409

    # Generate initials from full name or mine number
    if full_name:
        parts = full_name.split()
        initials = (parts[0][0] + parts[-1][0]).upper() if len(parts) >= 2 else parts[0][:2].upper()
    else:
        initials = mine_number[:2].upper()

    with get_conn() as conn:
        conn.execute(
            "INSERT INTO users (username,password,role,initials,email) VALUES (?,?,?,?,?)",
            (mine_number, pin, role, initials, email)
        )
    return jsonify({"ok": True, "username": mine_number, "role": role, "initials": initials})

@app.route("/api/login", methods=["POST"])
def login():
    d = request.json
    # Support both: mine_number+pin (new) and username+password (legacy builtin)
    mine_number = (d.get("mine_number") or d.get("username") or "").strip().upper()
    pin         = (d.get("pin") or d.get("password") or "").strip()
    # Try exact match first (uppercase mine number)
    u = lookup_user(mine_number)
    # Fallback: try lowercase original (for builtin demo users like fm_alice)
    if not u:
        original = (d.get("mine_number") or d.get("username") or "").strip()
        u = lookup_user(original)
        if u:
            mine_number = original  # use original key for builtin users
    if u and u["password"] == pin:
        session["username"] = mine_number
        session["role"]     = u["role"]
        session["initials"] = u["initials"]
        return jsonify({"ok": True, "username": mine_number, "role": u["role"], "initials": u["initials"]})
    return jsonify({"error": "Invalid mine number or PIN"}), 401

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

@app.route("/api/my_permissions")
def my_permissions():
    role  = session.get("role","")
    perms = get_perm(role)
    return jsonify({"role": role, "permissions": perms})

# ══════════════════════════════════════════════════════════════════════════════
# VENDORS
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/vendors", methods=["GET"])
def list_vendors():
    search = request.args.get("search","")
    month  = request.args.get("month","")
    sql    = "SELECT * FROM vendors WHERE 1=1"
    params = []
    if search: sql += " AND name LIKE ?"; params.append(f"%{search}%")
    if month:  sql += " AND created_month=?"; params.append(month)
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
    if not row: return jsonify({"error":"Not found"}), 404
    return jsonify(dict(row))

@app.route("/api/vendors", methods=["POST"])
def add_vendor():
    err = check_perm("can_add_vendor")
    if err: return err
    d   = request.json
    vid = new_id()
    cm  = date.today().strftime("%Y-%m")
    try:
        with get_conn() as conn:
            conn.execute(
                "INSERT INTO vendors (vendor_id,name,category,payment_terms,payment_method,bank_name,bank_account,ecocash_number,email,supplier_type,created_month) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (vid, d["name"], d["category"],
                 d.get("payment_terms",30), d.get("payment_method","Bank"),
                 d.get("bank_name",""), d.get("bank_account",""),
                 d.get("ecocash_number",""), d.get("email",""),
                 d.get("supplier_type","LOCAL"), cm)
            )
        # Email Finance Manager about new vendor
        _email_async(
            Emails.vendor_added,
            d["name"], d["category"], d.get("payment_method","Bank"),
            d.get("bank_account",""), session.get("username","system")
        )
        return jsonify({"ok":True,"vendor_id":vid}), 201
    except sqlite3.IntegrityError as e:
        return jsonify({"error":str(e)}), 400

# ══════════════════════════════════════════════════════════════════════════════
# INVOICES
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/invoices", methods=["GET"])
def list_invoices():
    status = request.args.get("status","")
    vendor = request.args.get("vendor_id","")
    month  = request.args.get("month","")
    search = request.args.get("search","")
    sql    = """SELECT i.*, v.name AS vendor_name, v.email AS vendor_email,
                       v.bank_account, v.ecocash_number, v.payment_method AS vendor_pay_method
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
    err = check_perm("can_create_invoice")
    if err: return err
    d      = request.json
    iid    = new_id()
    idate  = d["invoice_date"]
    imonth = idate[:7]
    try:
        with get_conn() as conn:
            conn.execute(
                "INSERT INTO invoices (invoice_id,invoice_number,vendor_id,invoice_date,due_date,description,total_amount,outstanding_amount,currency,cost_centre_number,cost_centre_name,doc_reference,created_by,invoice_month) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (iid, d["invoice_number"], d["vendor_id"], idate, d.get("due_date") or None,
                 d["description"], float(d["total_amount"]), float(d["total_amount"]),
                 d.get("currency","USD"), d.get("cost_centre_number") or None,
                 d.get("cost_centre_name",""), d.get("doc_reference",""),
                 d["created_by"], imonth)
            )
            log_action(conn, iid, "Created", None, "Draft", d["created_by"])
        return jsonify({"ok":True,"invoice_id":iid}), 201
    except sqlite3.IntegrityError as e:
        return jsonify({"error":str(e)}), 400

@app.route("/api/invoices/<iid>")
def get_invoice(iid):
    with get_conn() as conn:
        row = conn.execute(
            """SELECT i.*, v.name AS vendor_name, v.email AS vendor_email,
                      v.bank_account, v.ecocash_number
               FROM invoices i JOIN vendors v ON i.vendor_id=v.vendor_id
               WHERE i.invoice_id=?""", (iid,)
        ).fetchone()
        logs = conn.execute(
            "SELECT * FROM workflow_log WHERE invoice_id=? ORDER BY performed_at", (iid,)
        ).fetchall()
    if not row: return jsonify({"error":"Not found"}), 404
    d = dict(row); d["audit"] = rows_to_list(logs); d["age_bucket"] = age_bucket(d["invoice_date"])
    return jsonify(d)

# ── WORKFLOW — email is sent at every step ────────────────────────────────────
@app.route("/api/invoices/<iid>/workflow", methods=["POST"])
def workflow(iid):
    d            = request.json
    action       = d.get("action")
    performed_by = d.get("performed_by", session.get("username","system"))
    notes        = d.get("notes","")

    perm_map = {"submit":"can_submit","verify":"can_verify",
                "approve":"can_approve","reject":"can_reject"}
    if action in perm_map:
        err = check_perm(perm_map[action])
        if err: return err

    transitions = {
        "submit":  ("Draft",                 "Submitted"),
        "verify":  ("Submitted",             "Verified"),
        "approve": ("Verified",              "Approved"),
        "reject":  (("Submitted","Verified"),"Rejected"),
    }
    if action not in transitions:
        return jsonify({"error":"Invalid action"}), 400

    with get_conn() as conn:
        inv = conn.execute(
            """SELECT i.*, v.name AS vendor_name, v.email AS vendor_email,
                      v.cost_centre_name
               FROM invoices i JOIN vendors v ON i.vendor_id=v.vendor_id
               WHERE i.invoice_id=?""", (iid,)
        ).fetchone()
        if not inv: return jsonify({"error":"Invoice not found"}), 404

        allowed, to_status = transitions[action]
        if isinstance(allowed, str): allowed = (allowed,)
        if inv["status"] not in allowed:
            return jsonify({"error": f"Cannot {action}: status is '{inv['status']}'"}), 400

        rr = notes if action == "reject" else inv["rejection_reason"]
        conn.execute("UPDATE invoices SET status=?, rejection_reason=? WHERE invoice_id=?",
                     (to_status, rr, iid))
        log_action(conn, iid, action.capitalize(), inv["status"], to_status, performed_by, notes)

    # ── Send emails in background ─────────────────────────────────────────────
    inv = dict(inv)
    amount    = inv["total_amount"]
    inv_num   = inv["invoice_number"]
    v_name    = inv["vendor_name"]
    v_email   = inv.get("vendor_email","")
    inv_date  = inv["invoice_date"]
    desc      = inv["description"]
    cc        = f"{inv.get('cost_centre_number','')} {inv.get('cost_centre_name','')}".strip()

    if action == "submit":
        _email_async(Emails.invoice_submitted,
                     inv_num, v_name, amount, inv_date, desc, performed_by, cc)

    elif action == "verify":
        _email_async(Emails.invoice_verified,
                     inv_num, v_name, amount, performed_by, cc)

    elif action == "approve":
        _email_async(Emails.invoice_approved,
                     v_email, v_name, inv_num, amount, performed_by)

    elif action == "reject":
        _email_async(Emails.invoice_rejected,
                     v_email, v_name, inv_num, amount, notes, performed_by)

    return jsonify({"ok":True,"new_status":to_status})

# ══════════════════════════════════════════════════════════════════════════════
# PAYMENT BATCHES
# ══════════════════════════════════════════════════════════════════════════════

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
    err = check_perm("can_create_batch")
    if err: return err
    d          = request.json
    bid        = new_id()
    created_by = d.get("created_by", session.get("username","system"))
    items      = d.get("items", [])
    summaries  = []   # built for email
    try:
        with get_conn() as conn:
            conn.execute(
                "INSERT INTO payment_batches (batch_id,batch_reference,scheduled_date,notes,created_by) VALUES (?,?,?,?,?)",
                (bid, d["batch_reference"], d["scheduled_date"], d.get("notes",""), created_by)
            )
            for item in items:
                iid    = item["invoice_id"]
                amount = float(item["scheduled_amount"])
                inv    = conn.execute(
                    """SELECT i.*, v.name AS vendor_name, v.email AS vendor_email
                       FROM invoices i JOIN vendors v ON i.vendor_id=v.vendor_id
                       WHERE i.invoice_id=?""", (iid,)
                ).fetchone()
                if not inv:
                    return jsonify({"error": f"Invoice {iid} not found"}), 404
                if inv["status"] != "Approved":
                    return jsonify({"error": f"Invoice {inv['invoice_number']} must be Approved (current: {inv['status']})"}), 400
                if amount > inv["outstanding_amount"]:
                    return jsonify({"error": f"Amount for {inv['invoice_number']} exceeds outstanding balance"}), 400
                conn.execute(
                    "INSERT INTO batch_items (item_id,batch_id,invoice_id,scheduled_amount) VALUES (?,?,?,?)",
                    (new_id(), bid, iid, amount)
                )
                conn.execute("UPDATE invoices SET status='Scheduled' WHERE invoice_id=?", (iid,))
                log_action(conn, iid, "Scheduled", "Approved", "Scheduled", created_by, f"Batch {d['batch_reference']}")
                summaries.append({
                    "vendor_name":  inv["vendor_name"],
                    "vendor_email": inv["vendor_email"] or "",
                    "invoice_number": inv["invoice_number"],
                    "amount": amount,
                })

        total = sum(s["amount"] for s in summaries)
        _email_async(Emails.batch_created,
                     d["batch_reference"], d["scheduled_date"],
                     total, summaries, created_by)

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
    d        = request.json
    iid      = d["invoice_id"]
    amount   = float(d["scheduled_amount"])
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
        log_action(conn, iid, "Scheduled","Approved","Scheduled", added_by, f"Batch {bid[:8]}")
    return jsonify({"ok":True})

# ══════════════════════════════════════════════════════════════════════════════
# PAYMENTS — email confirmation to vendor
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/payments", methods=["POST"])
def record_payment():
    err = check_perm("can_record_payment")
    if err: return err
    d      = request.json
    iid    = d["invoice_id"]
    amount = float(d["amount_paid"])
    with get_conn() as conn:
        inv = conn.execute(
            """SELECT i.*, v.name AS vendor_name, v.email AS vendor_email
               FROM invoices i JOIN vendors v ON i.vendor_id=v.vendor_id
               WHERE i.invoice_id=?""", (iid,)
        ).fetchone()
        if not inv: return jsonify({"error":"Invoice not found"}), 404
        if inv["status"] not in ("Approved","Scheduled","Partially Paid"):
            return jsonify({"error":f"Cannot pay invoice with status '{inv['status']}'"}), 400
        if amount > inv["outstanding_amount"]:
            return jsonify({"error":"Payment exceeds outstanding balance"}), 400

        new_out    = round(inv["outstanding_amount"] - amount, 2)
        new_status = "Paid" if new_out == 0 else "Partially Paid"
        recorded_by = d.get("recorded_by", session.get("username","system"))

        conn.execute(
            "INSERT INTO payments (payment_id,invoice_id,batch_id,payment_date,amount_paid,payment_method,bank_reference,recorded_by) VALUES (?,?,?,?,?,?,?,?)",
            (new_id(), iid, d.get("batch_id"), d["payment_date"], amount,
             d.get("payment_method","Bank"), d.get("bank_reference",""), recorded_by)
        )
        conn.execute("UPDATE invoices SET outstanding_amount=?, status=? WHERE invoice_id=?",
                     (new_out, new_status, iid))
        log_action(conn, iid, f"Payment ({new_status})", inv["status"], new_status, recorded_by,
                   f"Paid:{amount} Method:{d.get('payment_method','Bank')} Ref:{d.get('bank_reference','')}")

    # Email payment confirmation to vendor
    _email_async(
        Emails.payment_confirmed,
        inv["vendor_email"] or "", inv["vendor_name"], inv["invoice_number"],
        amount, d["payment_date"], d.get("payment_method","Bank"),
        d.get("bank_reference",""), recorded_by
    )

    return jsonify({"ok":True, "new_status":new_status, "outstanding":new_out})

# ══════════════════════════════════════════════════════════════════════════════
# EMAIL LOG — view all sent emails in the admin UI
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/email_log")
def email_log():
    return jsonify(get_email_log(100))

@app.route("/api/email_status")
def email_status():
    return jsonify({
        "email_ready": EMAIL_READY,
        "email_enabled": EMAIL_ENABLED,
        "mode": ("LIVE" if EMAIL_ENABLED else "PREVIEW (set EMAIL_ENABLED=True in email_service.py)")
    })

# ══════════════════════════════════════════════════════════════════════════════
# EXPORT — CSV for Excel
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/export/invoices")
def export_invoices():
    from flask import Response
    import csv, io
    month  = request.args.get("month","")
    status = request.args.get("status","")
    sql    = """SELECT i.invoice_number, v.name AS vendor, v.category,
                       COALESCE(v.supplier_type,'LOCAL') AS supplier_type,
                       i.invoice_date, i.due_date, i.description,
                       i.total_amount, i.outstanding_amount,
                       i.cost_centre_number, i.cost_centre_name,
                       i.status, i.invoice_month
                FROM invoices i JOIN vendors v ON i.vendor_id=v.vendor_id WHERE 1=1"""
    params = []
    if month:  sql += " AND i.invoice_month=?";  params.append(month)
    if status: sql += " AND i.status=?";          params.append(status)
    sql += " ORDER BY i.invoice_date DESC"
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    out = io.StringIO()
    w   = csv.writer(out)
    w.writerow(["Invoice No","Vendor","Category","Supplier Type","Invoice Date","Due Date",
                "Description","Total Amount (USD)","Outstanding (USD)",
                "Cost Centre No","Cost Centre Name","Status","Month"])
    for r in rows:
        w.writerow([r["invoice_number"],r["vendor"],r["category"],r["supplier_type"],
                    r["invoice_date"],r["due_date"] or "",r["description"],
                    r["total_amount"],r["outstanding_amount"],
                    r["cost_centre_number"] or "",r["cost_centre_name"] or "",
                    r["status"],r["invoice_month"] or ""])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition":"attachment;filename=zcdc_invoices.csv"})

@app.route("/api/export/outstanding")
def export_outstanding():
    from flask import Response
    import csv, io
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT v.name, v.category, COALESCE(v.supplier_type,'LOCAL') AS supplier_type,
                   v.payment_method, v.bank_account, v.ecocash_number,
                   COUNT(i.invoice_id) AS invoice_count,
                   COALESCE(SUM(i.outstanding_amount),0) AS outstanding
            FROM invoices i JOIN vendors v ON i.vendor_id=v.vendor_id
            WHERE i.status NOT IN ('Paid','Rejected')
            GROUP BY v.vendor_id ORDER BY outstanding DESC
        """).fetchall()
    out = io.StringIO()
    w   = csv.writer(out)
    w.writerow(["Vendor","Category","Supplier Type","Payment Method",
                "Bank Account","EcoCash Number","Invoice Count","Outstanding (USD)"])
    for r in rows:
        w.writerow([r["name"],r["category"],r["supplier_type"],r["payment_method"],
                    r["bank_account"] or "",r["ecocash_number"] or "",
                    r["invoice_count"],r["outstanding"]])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition":"attachment;filename=zcdc_outstanding.csv"})



# ══════════════════════════════════════════════════════════════════════════════
# DASHBOARD — main KPI summary
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/reports/dashboard")
def dashboard():
    with get_conn() as conn:
        # Totals
        total_outstanding = conn.execute(
            "SELECT COALESCE(SUM(outstanding_amount),0) AS v FROM invoices WHERE status NOT IN ('Paid','Rejected')"
        ).fetchone()["v"]
        pending_approval = conn.execute(
            "SELECT COUNT(*) AS v FROM invoices WHERE status IN ('Draft','Submitted','Verified')"
        ).fetchone()["v"]
        approved_unscheduled = conn.execute(
            "SELECT COUNT(*) AS v FROM invoices WHERE status='Approved'"
        ).fetchone()["v"]
        scheduled_total = conn.execute(
            "SELECT COALESCE(SUM(outstanding_amount),0) AS v FROM invoices WHERE status='Scheduled'"
        ).fetchone()["v"]
        paid_this_month = conn.execute(
            "SELECT COALESCE(SUM(amount_paid),0) AS v FROM payments WHERE strftime('%Y-%m',payment_date)=strftime('%Y-%m','now')"
        ).fetchone()["v"]
        total_vendors = conn.execute("SELECT COUNT(*) AS v FROM vendors").fetchone()["v"]

        # Overdue buckets
        overdue_31_60 = conn.execute(
            "SELECT COUNT(*) AS v FROM invoices WHERE status NOT IN ('Paid','Rejected') AND julianday('now')-julianday(invoice_date) BETWEEN 31 AND 60"
        ).fetchone()["v"]
        overdue_61_90 = conn.execute(
            "SELECT COUNT(*) AS v FROM invoices WHERE status NOT IN ('Paid','Rejected') AND julianday('now')-julianday(invoice_date) BETWEEN 61 AND 90"
        ).fetchone()["v"]
        overdue_91plus = conn.execute(
            "SELECT COUNT(*) AS v FROM invoices WHERE status NOT IN ('Paid','Rejected') AND julianday('now')-julianday(invoice_date) > 90"
        ).fetchone()["v"]

        # USD / ZiG split
        outstanding_usd = conn.execute(
            "SELECT COALESCE(SUM(outstanding_amount),0) AS v FROM invoices WHERE status NOT IN ('Paid','Rejected') AND (currency='USD' OR currency IS NULL OR currency='')"
        ).fetchone()["v"]
        outstanding_zig = conn.execute(
            "SELECT COALESCE(SUM(outstanding_amount),0) AS v FROM invoices WHERE status NOT IN ('Paid','Rejected') AND currency='ZiG'"
        ).fetchone()["v"]
        invoices_usd = conn.execute(
            "SELECT COUNT(*) AS v FROM invoices WHERE status NOT IN ('Paid','Rejected') AND (currency='USD' OR currency IS NULL OR currency='')"
        ).fetchone()["v"]
        invoices_zig = conn.execute(
            "SELECT COUNT(*) AS v FROM invoices WHERE status NOT IN ('Paid','Rejected') AND currency='ZiG'"
        ).fetchone()["v"]
        paid_usd_month = conn.execute(
            "SELECT COALESCE(SUM(p.amount_paid),0) AS v FROM payments p JOIN invoices i ON p.invoice_id=i.invoice_id WHERE strftime('%Y-%m',p.payment_date)=strftime('%Y-%m','now') AND (i.currency='USD' OR i.currency IS NULL OR i.currency='')"
        ).fetchone()["v"]
        paid_zig_month = conn.execute(
            "SELECT COALESCE(SUM(p.amount_paid),0) AS v FROM payments p JOIN invoices i ON p.invoice_id=i.invoice_id WHERE strftime('%Y-%m',p.payment_date)=strftime('%Y-%m','now') AND i.currency='ZiG'"
        ).fetchone()["v"]

        # Top vendors by outstanding
        top_vendors = conn.execute(
            """SELECT v.name AS vendor, v.payment_method, v.bank_account,
                      COALESCE(i.currency,'USD') AS currency,
                      COALESCE(SUM(i.outstanding_amount),0) AS total_outstanding,
                      COUNT(i.invoice_id) AS invoice_count
               FROM invoices i JOIN vendors v ON i.vendor_id=v.vendor_id
               WHERE i.status NOT IN ('Paid','Rejected')
               GROUP BY v.vendor_id ORDER BY total_outstanding DESC LIMIT 8"""
        ).fetchall()

        # Status breakdown for chart
        status_rows = conn.execute(
            "SELECT status, COUNT(*) AS cnt FROM invoices GROUP BY status"
        ).fetchall()

        # Aging chart data
        aging_chart = conn.execute(
            """SELECT
               COALESCE(SUM(CASE WHEN julianday('now')-julianday(invoice_date)<=30 THEN outstanding_amount END),0) AS b0_30,
               COALESCE(SUM(CASE WHEN julianday('now')-julianday(invoice_date) BETWEEN 31 AND 60 THEN outstanding_amount END),0) AS b31_60,
               COALESCE(SUM(CASE WHEN julianday('now')-julianday(invoice_date) BETWEEN 61 AND 90 THEN outstanding_amount END),0) AS b61_90,
               COALESCE(SUM(CASE WHEN julianday('now')-julianday(invoice_date)>90 THEN outstanding_amount END),0) AS b91plus
               FROM invoices WHERE status NOT IN ('Paid','Rejected')"""
        ).fetchone()

        # Recent invoices for table
        recent_invoices = conn.execute(
            """SELECT i.invoice_number, v.name AS vendor_name, i.invoice_date,
                      i.total_amount, i.outstanding_amount, i.status,
                      COALESCE(i.currency,'USD') AS currency,
                      julianday('now')-julianday(i.invoice_date) AS age_days
               FROM invoices i JOIN vendors v ON i.vendor_id=v.vendor_id
               ORDER BY i.created_at DESC LIMIT 10"""
        ).fetchall()

    return jsonify({
        "total_outstanding":     total_outstanding,
        "pending_approval":      pending_approval,
        "approved_unscheduled":  approved_unscheduled,
        "scheduled_total":       scheduled_total,
        "paid_this_month":       paid_this_month,
        "total_vendors":         total_vendors,
        "overdue_31_60":         overdue_31_60,
        "overdue_61_90":         overdue_61_90,
        "overdue_91plus":        overdue_91plus,
        "outstanding_usd":       outstanding_usd,
        "outstanding_zig":       outstanding_zig,
        "invoices_usd":          invoices_usd,
        "invoices_zig":          invoices_zig,
        "paid_usd_month":        paid_usd_month,
        "paid_zig_month":        paid_zig_month,
        "top_vendors":           rows_to_list(top_vendors),
        "status_breakdown":      {r["status"]: r["cnt"] for r in status_rows},
        "aging_chart":           dict(aging_chart) if aging_chart else {},
        "recent_invoices":       rows_to_list(recent_invoices),
    })

# ══════════════════════════════════════════════════════════════════════════════
# OUTSTANDING REPORT
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/reports/outstanding")
def outstanding_report():
    month    = request.args.get("month","")
    category = request.args.get("category","")
    sql = """SELECT v.vendor_id, v.name AS vendor, v.category,
                    COALESCE(v.supplier_type,'LOCAL') AS supplier_type,
                    v.payment_method, v.bank_account, v.ecocash_number,
                    COALESCE(i.currency,'USD') AS currency,
                    COUNT(i.invoice_id) AS invoice_count,
                    COALESCE(SUM(i.outstanding_amount),0) AS total_outstanding
             FROM invoices i JOIN vendors v ON i.vendor_id=v.vendor_id
             WHERE i.status NOT IN ('Paid','Rejected')"""
    params = []
    if month:    sql += " AND i.invoice_month=?";   params.append(month)
    if category: sql += " AND v.category=?";         params.append(category)
    sql += " GROUP BY v.vendor_id, i.currency ORDER BY total_outstanding DESC"
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
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
        conn.execute("INSERT INTO vendors (vendor_id,name,category,payment_terms,payment_method,bank_name,bank_account,ecocash_number,created_month,supplier_type) VALUES (?,?,?,?,?,?,?,?,?,?)",
                     (v1,"ABC Electrical Supplies","Supplies",30,"Bank","CABS","ACC-1234567","",m1,"LOCAL"))
        conn.execute("INSERT INTO vendors (vendor_id,name,category,payment_terms,payment_method,bank_name,bank_account,ecocash_number,created_month,supplier_type) VALUES (?,?,?,?,?,?,?,?,?,?)",
                     (v2,"XYZ Maintenance Services","Services",45,"EcoCash","","","0771234567",m0,"ZCDC"))
        conn.execute("INSERT INTO vendors (vendor_id,name,category,payment_terms,payment_method,bank_name,bank_account,ecocash_number,created_month,supplier_type) VALUES (?,?,?,?,?,?,?,?,?,?)",
                     (v3,"Rapid Logistics Ltd","Services",14,"Bank","ZB","ACC-7654321","",m2,"LOCAL"))
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



# ── NEW TABLES (added to init_db via ALTER TABLE) ─────────────────────────────
def _ensure_new_tables():
    """Create new tables and columns if they do not already exist."""
    with get_conn() as conn:
        # supplier_type column on vendors
        try:
            conn.execute("ALTER TABLE vendors ADD COLUMN supplier_type TEXT DEFAULT 'LOCAL'")
        except Exception:
            pass
        # tax_invoices
        conn.execute("""CREATE TABLE IF NOT EXISTS tax_invoices (
            tax_invoice_id TEXT PRIMARY KEY,
            vendor_id      TEXT NOT NULL REFERENCES vendors(vendor_id),
            invoice_number TEXT NOT NULL,
            invoice_date   TEXT NOT NULL,
            tax_type       TEXT DEFAULT 'VAT',
            gross_amount   REAL NOT NULL,
            vat_amount     REAL DEFAULT 0,
            net_amount     REAL NOT NULL,
            currency       TEXT DEFAULT 'USD',
            exchange_rate  REAL DEFAULT 1,
            amount_usd     REAL NOT NULL,
            description    TEXT,
            captured_by    TEXT,
            status         TEXT DEFAULT 'Captured',
            created_at     TEXT DEFAULT (datetime('now'))
        )""")
        # notification_log
        conn.execute("""CREATE TABLE IF NOT EXISTS notification_log (
            notif_id  TEXT PRIMARY KEY,
            type      TEXT NOT NULL,
            target    TEXT,
            message   TEXT,
            sent_by   TEXT,
            sent_at   TEXT DEFAULT (datetime('now'))
        )""")

_ensure_new_tables()


# ── VENDOR RANKING ─────────────────────────────────────────────────────────────
@app.route("/api/analytics/vendor_ranking")
def vendor_ranking():
    sort_by = request.args.get("sort_by", "outstanding")
    supplier_type = request.args.get("type", "")
    with get_conn() as conn:
        vendors = conn.execute("SELECT * FROM vendors").fetchall()
        results = []
        for v in vendors:
            vid = v["vendor_id"]
            outstanding = conn.execute(
                "SELECT COALESCE(SUM(outstanding_amount),0) AS v FROM invoices WHERE vendor_id=? AND status NOT IN ('Paid','Rejected')", (vid,)
            ).fetchone()["v"]
            paid = conn.execute(
                "SELECT COALESCE(SUM(p.amount_paid),0) AS v FROM payments p JOIN invoices i ON p.invoice_id=i.invoice_id WHERE i.vendor_id=?", (vid,)
            ).fetchone()["v"]
            inv_count = conn.execute(
                "SELECT COUNT(*) AS v FROM invoices WHERE vendor_id=?", (vid,)
            ).fetchone()["v"]
            total_invoiced = conn.execute(
                "SELECT COALESCE(SUM(total_amount),0) AS v FROM invoices WHERE vendor_id=? AND status!='Rejected'", (vid,)
            ).fetchone()["v"]
            results.append({
                "vendor_id": vid, "vendor": v["name"],
                "category": v["category"],
                "supplier_type": v["supplier_type"] if "supplier_type" in v.keys() else "LOCAL",
                "payment_method": v["payment_method"],
                "bank_account": v["bank_account"] or "",
                "outstanding": outstanding, "total_paid": paid,
                "invoice_count": inv_count, "total_invoiced": total_invoiced,
            })
        if supplier_type:
            results = [r for r in results if r["supplier_type"] == supplier_type]
        sort_map = {"outstanding": "outstanding", "paid": "total_paid", "invoices": "invoice_count"}
        results.sort(key=lambda x: -x.get(sort_map.get(sort_by, "outstanding"), 0))
    return jsonify(results)


# ── TAX INVOICES ──────────────────────────────────────────────────────────────
@app.route("/api/tax_invoices", methods=["GET"])
def list_tax_invoices():
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT ti.*, v.name AS vendor_name FROM tax_invoices ti
               JOIN vendors v ON ti.vendor_id=v.vendor_id
               ORDER BY ti.invoice_date DESC"""
        ).fetchall()
    return jsonify(rows_to_list(rows))

@app.route("/api/tax_invoices", methods=["POST"])
def add_tax_invoice():
    d = request.json
    tid = new_id()
    gross = float(d.get("gross_amount", 0))
    vat   = float(d.get("vat_amount", 0))
    net   = float(d.get("net_amount", gross - vat))
    rate  = float(d.get("exchange_rate", 1))
    currency = d.get("currency", "USD")
    usd = net if currency == "USD" else (net / rate if rate else net)
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO tax_invoices
            (tax_invoice_id,vendor_id,invoice_number,invoice_date,tax_type,
             gross_amount,vat_amount,net_amount,currency,exchange_rate,
             amount_usd,description,captured_by,status)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (tid, d["vendor_id"], d["invoice_number"], d["invoice_date"],
              d.get("tax_type","VAT"), gross, vat, net, currency, rate,
              round(usd, 4), d.get("description",""),
              d.get("captured_by", session.get("username","system")), "Captured"))
    return jsonify({"ok": True, "tax_invoice_id": tid}), 201


# ── KPI TRACKING ──────────────────────────────────────────────────────────────
@app.route("/api/kpis")
def get_kpis():
    with get_conn() as conn:
        total_inv   = conn.execute("SELECT COUNT(*) AS v FROM invoices").fetchone()["v"] or 1
        total_paid  = conn.execute("SELECT COUNT(*) AS v FROM payments").fetchone()["v"] or 0
        paid_on_time = conn.execute(
            """SELECT COUNT(*) AS v FROM payments p
               JOIN invoices i ON p.invoice_id=i.invoice_id
               JOIN vendors v ON i.vendor_id=v.vendor_id
               WHERE julianday(p.payment_date) <= julianday(i.invoice_date)+v.payment_terms"""
        ).fetchone()["v"]
        avg_cycle = conn.execute(
            """SELECT AVG(julianday(wl.performed_at)-julianday(i.created_at)) AS v
               FROM invoices i
               JOIN workflow_log wl ON wl.invoice_id=i.invoice_id AND wl.to_status='Approved'"""
        ).fetchone()["v"] or 0
        rejected = conn.execute("SELECT COUNT(*) AS v FROM invoices WHERE status='Rejected'").fetchone()["v"]
        tax_captured = conn.execute("SELECT COUNT(*) AS v FROM tax_invoices").fetchone()["v"]
    otp_rate = round(paid_on_time / total_paid * 100, 1) if total_paid else 0
    rej_rate = round(rejected / total_inv * 100, 1)
    return jsonify({"kpis": [
        {"name":"On-Time Payment Rate","value":otp_rate,"unit":"%","target":90,
         "status":"green" if otp_rate>=90 else "amber" if otp_rate>=70 else "red"},
        {"name":"Avg Approval Cycle","value":round(avg_cycle,1),"unit":" days","target":5,
         "status":"green" if avg_cycle<=5 else "red"},
        {"name":"Invoice Rejection Rate","value":rej_rate,"unit":"%","target":5,
         "status":"green" if rej_rate<=5 else "red"},
        {"name":"Tax Invoices Captured","value":tax_captured,"unit":"","target":None,"status":"info"},
    ]})


# ── NOTIFICATIONS ─────────────────────────────────────────────────────────────
@app.route("/api/notifications/send", methods=["POST"])
def send_notification():
    d        = request.json
    nid      = new_id()
    notif_type = d.get("type","general")
    target   = d.get("target","")          # role name, email address, or "all"
    message  = d.get("message","")
    subject  = d.get("subject","") or f"ZCDC Vendor Payments — Notification"
    sent_by  = session.get("username","system")
    extra_emails = [e.strip() for e in d.get("extra_emails","").split(",") if e.strip() and "@" in e.strip()]

    # ── Resolve who to email ───────────────────────────────────────────────────
    recipients = []

    # If target looks like an email address, send directly
    if target and "@" in target:
        recipients.append(target)
    # If target is "all", send to every configured role
    elif target == "all":
        from email_service import ROLE_EMAILS
        recipients = list(set(ROLE_EMAILS.values()))
    # If target matches a role name, look up that role's email
    elif target:
        from email_service import ROLE_EMAILS
        role_email = ROLE_EMAILS.get(target)
        if role_email:
            recipients.append(role_email)

    # Add any extra emails typed into the form
    for em in extra_emails:
        if em not in recipients:
            recipients.append(em)

    # Remove blanks
    recipients = [r for r in recipients if r and "@" in r]

    # ── Build HTML email body ──────────────────────────────────────────────────
    GOLD = "#C8960C"
    html_body = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<style>
  body{{font-family:Arial,sans-serif;background:#f4f4f4;margin:0;padding:20px}}
  .wrap{{max-width:600px;margin:0 auto;background:#fff;border-radius:10px;overflow:hidden}}
  .header{{background:linear-gradient(135deg,#1a1200,#3d2e00);padding:24px 30px;text-align:center}}
  .mark{{font-size:10px;font-weight:700;letter-spacing:.2em;color:{GOLD};text-transform:uppercase}}
  .title{{font-size:20px;font-weight:700;color:#F0C040;margin-top:4px}}
  .body{{padding:26px 30px;color:#222;font-size:14px;line-height:1.7}}
  .message-box{{background:#f9f5e7;border-left:4px solid {GOLD};border-radius:6px;padding:14px 18px;margin:16px 0;font-size:14px}}
  .footer{{background:#1a1200;padding:14px 30px;text-align:center;font-size:11px;color:#7a6a40}}
  .footer strong{{color:{GOLD}}}
</style></head>
<body><div class="wrap">
  <div class="header">
    <div class="mark">ZCDC</div>
    <div class="title">Vendor Payments System</div>
  </div>
  <div class="body">
    <p>You have received the following notification from the <strong>ZCDC Vendor Payments System</strong>:</p>
    <div class="message-box">{message}</div>
    <p style="font-size:12px;color:#888">Sent by: <strong>{sent_by}</strong> · Type: {notif_type}</p>
  </div>
  <div class="footer">
    <p>Automated notification from <strong>ZCDC Vendor Payments System</strong><br>
    Do not reply to this email. Contact the Finance Department for queries.</p>
  </div>
</div></body></html>"""

    # ── Send emails ────────────────────────────────────────────────────────────
    email_results = []
    if recipients and EMAIL_READY:
        from email_service import send_email
        for addr in recipients:
            result = send_email(addr, subject, html_body, plain_body=message)
            email_results.append({"to": addr, "success": result.get("success"), "msg": result.get("message")})
    elif recipients and not EMAIL_READY:
        email_results = [{"to": r, "success": False, "msg": "email_service.py not loaded"} for r in recipients]

    # ── Log it ─────────────────────────────────────────────────────────────────
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO notification_log (notif_id,type,target,message,sent_by) VALUES (?,?,?,?,?)",
            (nid, notif_type, ", ".join(recipients) if recipients else target, message, sent_by)
        )

    success_count = sum(1 for r in email_results if r.get("success"))
    return jsonify({
        "ok": True,
        "notif_id": nid,
        "recipients": recipients,
        "email_results": email_results,
        "message": (f"✅ Email sent to {success_count}/{len(recipients)} recipient(s): {', '.join(recipients)}"
                    if recipients else
                    f"⚠️ No email addresses found for target '{target}'. Check ROLE_EMAILS in email_service.py.")
    })

@app.route("/api/notifications/log")
def notification_log():
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM notification_log ORDER BY sent_at DESC LIMIT 50"
        ).fetchall()
    return jsonify(rows_to_list(rows))


@app.route("/api/export/tax_invoices")
def export_tax_invoices():
    from flask import Response
    import csv, io
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT ti.*, v.name AS vendor_name FROM tax_invoices ti
            JOIN vendors v ON ti.vendor_id=v.vendor_id
            ORDER BY ti.invoice_date DESC
        """).fetchall()
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Invoice #","Vendor","Date","Tax Type","Currency",
                "Gross Amount","VAT Amount","Net Amount","Exchange Rate","USD Amount","Description","Captured By"])
    for r in rows:
        w.writerow([r["invoice_number"], r["vendor_name"], r["invoice_date"],
                    r["tax_type"], r["currency"], r["gross_amount"],
                    r["vat_amount"], r["net_amount"], r["exchange_rate"],
                    r["amount_usd"], r["description"] or "", r["captured_by"] or ""])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition":"attachment;filename=zcdc_tax_invoices.csv"})

@app.route("/api/export/aging")
def export_aging():
    from flask import Response
    import csv, io
    month = request.args.get("month","")
    sql = "SELECT v.name AS vendor, i.invoice_date, i.outstanding_amount FROM invoices i JOIN vendors v ON i.vendor_id=v.vendor_id WHERE i.status NOT IN ('Paid','Rejected')"
    params = []
    if month: sql += " AND i.invoice_month=?"; params.append(month)
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    from collections import defaultdict
    buckets = ["0-30 days","31-60 days","61-90 days","91+ days"]
    vd = defaultdict(lambda: defaultdict(float))
    for r in rows:
        b = age_bucket(r["invoice_date"])
        vd[r["vendor"]][b] += r["outstanding_amount"]
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Vendor"] + buckets + ["Total"])
    for vendor, bdata in sorted(vd.items(), key=lambda x: -sum(x[1].values())):
        total = sum(bdata.values())
        w.writerow([vendor] + [bdata.get(b,0) for b in buckets] + [total])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition":"attachment;filename=zcdc_aging.csv"})

@app.route("/api/export/vendor_ranking")
def export_vendor_ranking():
    from flask import Response
    import csv, io
    sort_by = request.args.get("sort_by","outstanding")
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT v.name, v.category, COALESCE(v.supplier_type,'LOCAL') AS supplier_type,
                   v.payment_method, v.bank_account,
                   COALESCE(SUM(CASE WHEN i.status NOT IN ('Paid','Rejected') THEN i.outstanding_amount ELSE 0 END),0) AS outstanding,
                   COALESCE(SUM(i.total_amount),0) AS total_invoiced,
                   COUNT(i.invoice_id) AS invoice_count
            FROM vendors v LEFT JOIN invoices i ON v.vendor_id=i.vendor_id
            GROUP BY v.vendor_id ORDER BY outstanding DESC
        """).fetchall()
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Rank","Vendor","Supplier Type","Category","Payment Method","Bank Account","Total Invoiced","Outstanding","Invoice Count"])
    for i, r in enumerate(rows, 1):
        w.writerow([i, r["name"], r["supplier_type"], r["category"],
                    r["payment_method"], r["bank_account"] or "",
                    r["total_invoiced"], r["outstanding"], r["invoice_count"]])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition":"attachment;filename=zcdc_vendor_ranking.csv"})

# ── SOP ROUTES (IMS-FIN-SOP-01 & IMS-FIN-SOP-02) ────────────────────────────

@app.route("/api/sops")
def get_sops():
    """Return full SOP data from IMS-FIN-SOP-01 and IMS-FIN-SOP-02."""
    sops = [
        {
            "code": "IMS-FIN-SOP-01",
            "title": "Accounts Payable: Invoice Processing",
            "version": "Rev 1 — 31 Jan 2025",
            "objective": "Ensure supplier invoices are timely received and captured for goods and services received.",
            "scope": "Receiving and processing of invoices including erroneous invoice handling.",
            "prohibitions": [
                "Processing invoices without checking authorization of Purchase Order, Supplier Contract and Goods Receipt Note."
            ],
            "key_risks": [
                {"ref":"Fin14","hazard":"Posting to wrong supplier account","risk":"Overpayment / Misstated financial records","control":"Use of ERP · Segregation of duties · Creditors reconciliations"},
                {"ref":"Fin16","hazard":"Omission of invoices","risk":"Misstated Creditors ledger balance","control":"Creditors reconciliations · Peer reviews"},
            ],
            "kpis": [
                {"name":"Timely and accurate capturing of invoices","activity":"Capturing invoices accurately and timely as part of invoice processing"}
            ],
            "roles": "Receiving Clerk · Cost and Management Clerk · Cost and Management Accountant · Finance Manager · Chief Finance Officer",
            "steps": [
                {
                    "step": 1,
                    "action": "Receive Invoice from Supplier",
                    "detail": "Receive invoice from supplier through mail or in person. Record in the Invoice Receipt Register: Supplier Name, Invoice Number, Date Received, Quantity, Name of person who received it.",
                    "responsible": ["Receiving Clerk","Cost and Management Clerk"],
                    "system_action": "Create invoice record in Draft status with all supporting references.",
                    "status_transition": None,
                },
                {
                    "step": 2,
                    "action": "Attach Supporting Documents",
                    "detail": "Attach all supporting documents: Purchase Order/Contract, Approved Goods Received Form, Approved Delivery Note, or signed Payment Certificate/Job Card for services. Send invoice and register to Cost and Management Clerk.",
                    "responsible": ["Receiving Clerk","Cost and Management Clerk"],
                    "system_action": "Enter doc reference (PO number) on invoice record.",
                    "status_transition": None,
                },
                {
                    "step": 3,
                    "action": "Check Invoice Receipt Register & Acknowledge",
                    "detail": "Check and sign the Invoice Receipt Register as acknowledgment of receipt. Verify Purchase Order, Vendor Contract, and Goods Receipt Note were appropriately authorised.",
                    "responsible": ["Cost and Management Clerk"],
                    "system_action": "Submit invoice (Draft → Submitted).",
                    "status_transition": "Draft → Submitted",
                },
                {
                    "step": 4,
                    "action": "Check Tax Compliance",
                    "detail": "Invoices shall be checked for ZIMRA tax compliance. If not tax compliant, inform supplier to provide a correct invoice. For Tax Invoices, capture VAT and WHT amounts.",
                    "responsible": ["Cost and Management Clerk"],
                    "system_action": "Select Invoice Type = 'Tax Invoice' and enter VAT/WHT amounts if applicable. Reject (Draft) if non-compliant.",
                    "status_transition": "Reject if non-compliant",
                },
                {
                    "step": 5,
                    "action": "Check Vendor Account in System",
                    "detail": "Check if the vendor account exists in the system. If vendor does not exist: determine if transaction is Ad hoc or for stock items. If Ad hoc — check approved manual PO & GRV. If for inventory — send back to Stores to create vendor account.",
                    "responsible": ["Cost and Management Clerk"],
                    "system_action": "Search vendor master. Add new vendor if needed via Vendor Master module.",
                    "status_transition": None,
                },
                {
                    "step": 6,
                    "action": "Three-Way Match (Invoice vs PO vs GRV)",
                    "detail": "Record the invoice against the Purchase Order and Goods Receipt. System matches Invoice, PO, and GRV to ensure price and quantity agree. If invoice exceeds PO amount — send back to vendor to amend before processing.",
                    "responsible": ["Cost and Management Clerk"],
                    "system_action": "Enter PO reference and GRV reference. System flags if amount exceeds PO. Duplicate invoice number prevention active.",
                    "status_transition": None,
                },
                {
                    "step": 7,
                    "action": "Park Invoice in System",
                    "detail": "If invoice passes the three-way match — 'Park' the invoice in the system and submit supporting documents to the Cost and Management Accountant to check accuracy.",
                    "responsible": ["Cost and Management Clerk"],
                    "system_action": "Invoice moves to 'Submitted' status (parked) — awaiting Accountant review.",
                    "status_transition": "Submitted (Parked)",
                },
                {
                    "step": 8,
                    "action": "Review Parked Invoices (Daily)",
                    "detail": "On a daily basis, the Cost and Management Accountant reviews all parked invoices. Both Accountant and Clerk review the invoice ensuring all details pass the 3-way match and information is accurate per supporting documents. Check tolerance levels.",
                    "responsible": ["Cost and Management Accountant","Cost and Management Clerk"],
                    "system_action": "Review invoice in Approval Backlog. Verify details match PO and GRV.",
                    "status_transition": None,
                },
                {
                    "step": 9,
                    "action": "Post Invoice (Verify & Approve)",
                    "detail": "Once satisfied, 'Post' the invoice in the system. Stamp processed invoices as 'Processed' to avoid duplication. Send processed invoices to Finance Head Office for payment.",
                    "responsible": ["Cost and Management Accountant"],
                    "system_action": "Verify invoice (Submitted → Verified). Finance Manager then Approves (Verified → Approved).",
                    "status_transition": "Verified → Approved",
                },
                {
                    "step": 10,
                    "action": "Invoice Reversal / Credit Notes",
                    "detail": "State reason for reversal with supporting docs (e.g. credit note). Assistant Finance Manager reviews and approves reversal. For credit notes: Clerk requests from vendor, parks in system, Accountant reviews and posts.",
                    "responsible": ["Cost and Management Clerk","Cost and Management Accountant","Assistant Finance Manager"],
                    "system_action": "Use 'Credit Note' invoice type. Enter reversal reason. Requires Assistant Finance Manager approval.",
                    "status_transition": "Reject workflow",
                },
            ],
        },
        {
            "code": "IMS-FIN-SOP-02",
            "title": "Accounts Payable: Period End Routines",
            "version": "Rev 1 — 31 Jan 2025",
            "objective": "Ensure AP balances are accurately recorded and suppliers are accurately and timely paid for outstanding balances.",
            "scope": "Supplier reconciliations and month-end reconciliations between GL and AP sub-ledger.",
            "prohibitions": [
                "Paying outstanding suppliers before receiving statements.",
                "Doing business with unapproved and unauthorised suppliers."
            ],
            "key_risks": [
                {"ref":"Fin11","hazard":"Misstated or overstated invoices from suppliers","risk":"Liquidity problems","control":"Monthly creditors reconciliations"},
                {"ref":"Fin12","hazard":"Fictitious vendors/suppliers","risk":"Wrong supplies/goods","control":"Proper examination of supplier tenders, invoices and statements"},
            ],
            "kpis": [
                {"name":"Timely performance of month end reconciliations","activity":"Conduct timely performance of month end reconciliations"}
            ],
            "roles": "Assistant Cost and Management Accountant · Assistant Finance Manager · Cost and Management Accountant · Finance Manager · Chief Finance Officer",
            "steps": [
                {
                    "step": 1,
                    "action": "Generate Creditors Ageing Listing",
                    "detail": "At month end, generate the creditors ageing listing to confirm outstanding credit balances.",
                    "responsible": ["Assistant Cost and Management Accountant","Cost and Management Accountant"],
                    "system_action": "Run Aging Analysis report from the Reports section. Export to CSV for supporting documentation.",
                    "status_transition": None,
                },
                {
                    "step": 2,
                    "action": "Obtain Supplier Statements",
                    "detail": "For outstanding supplier payments, check that a statement has been received from ZCDC. If not received — contact service provider by phone to request supplier statement.",
                    "responsible": ["Assistant Cost and Management Accountant"],
                    "system_action": "Use Outstanding Summary report to identify vendors requiring statements.",
                    "status_transition": None,
                },
                {
                    "step": 3,
                    "action": "Prepare Creditors Reconciliation",
                    "detail": "Prepare creditors reconciliation using the standard creditor reconciliation template. Ensure balance per SAP agrees to supplier balance per creditor statement. Follow up on disputed invoices, credit notes, debit notes or any reconciling items within 30 days.",
                    "responsible": ["Assistant Cost and Management Accountant"],
                    "system_action": "Export Outstanding Summary by vendor. Compare against supplier statement. Note reconciling items.",
                    "status_transition": None,
                },
                {
                    "step": 4,
                    "action": "Review Creditors Reconciliation",
                    "detail": "Assistant Finance Manager reviews the reconciliation — ensures supplier statement, GL printout, and support on reconciling items are all attached. Signs off as evidence of review.",
                    "responsible": ["Assistant Finance Manager"],
                    "system_action": "Use Approval Backlog to confirm all invoices on statement are in system.",
                    "status_transition": None,
                },
                {
                    "step": 5,
                    "action": "Prepare Payment Voucher",
                    "detail": "Once satisfied with amounts owing to respective suppliers, prepare payment voucher for corresponding amounts. Review and sign payment voucher as evidence of review.",
                    "responsible": ["Assistant Finance Manager","Cost and Management Accountant"],
                    "system_action": "Create Payment Batch in system. Add all approved invoices to batch. Note batch reference as voucher reference.",
                    "status_transition": "Approved → Scheduled",
                },
                {
                    "step": 6,
                    "action": "Prepare Consolidated Listing of Liabilities",
                    "detail": "Prepare consolidated listing/schedule of liabilities approved for payment. Review the consolidated listing.",
                    "responsible": ["Assistant Cost and Management Accountant","Assistant Finance Manager"],
                    "system_action": "Export Payment Schedule report showing all scheduled invoices grouped by batch.",
                    "status_transition": None,
                },
                {
                    "step": 7,
                    "action": "Generate Payment Run & Upload to Paynet",
                    "detail": "Generate payment run in system and upload into Paynet. Payment run approved in Paynet and payment automatically released.",
                    "responsible": ["Assistant Cost and Management Accountant","Treasury Officer"],
                    "system_action": "Use Payment Batches module. Record payment against each invoice. Use Bank Integration to simulate/send payment.",
                    "status_transition": "Scheduled → Paid",
                },
                {
                    "step": 8,
                    "action": "GL Reconciliation — Print AP GL & Sub-Ledger",
                    "detail": "At month end print AP general ledger and AP age analysis/sub-ledger. Using ZCDC GL reconciliation template, capture balances per GL and Sub-ledger.",
                    "responsible": ["Assistant Cost and Management Accountant"],
                    "system_action": "Export Aging Analysis and Outstanding Summary CSV reports for GL reconciliation workpapers.",
                    "status_transition": None,
                },
                {
                    "step": 9,
                    "action": "Agree GL vs Sub-Ledger Balances",
                    "detail": "Agree the balances and ensure no variances. If variances noted — investigate and rectify through journal entries where possible. Provide explanations for variances on the reconciliation.",
                    "responsible": ["Assistant Cost and Management Accountant"],
                    "system_action": "Compare exported totals. Flag any invoices in system not matching GL.",
                    "status_transition": None,
                },
                {
                    "step": 10,
                    "action": "Finance Manager Review & Sign-Off",
                    "detail": "Forward reconciliations with all supporting documents to the Finance Manager for review. Finance Manager reviews against supporting documents, signs off as evidence of review and approval. Files approved documents for record keeping and audit trail.",
                    "responsible": ["Finance Manager"],
                    "system_action": "Period-end reconciliation complete. All records stored in system audit trail.",
                    "status_transition": None,
                },
                {
                    "step": 11,
                    "action": "Mining Rehabilitation Provisions (Monthly)",
                    "detail": "Costing Clerk determines mine rehabilitation provision from SHE Manager and Senior Geologist inputs. Reviews for reasonability per accounting standards. Assistant Accountant creates and parks provision journal. Posts in system.",
                    "responsible": ["Cost and Management Accountant","Assistant Cost and Management Accountant"],
                    "system_action": "Capture as a provisions entry. Note in description: 'Mine Rehabilitation Provision — [Month]'.",
                    "status_transition": None,
                },
            ],
        },
    ]
    return jsonify(sops)

@app.route("/api/role_permissions")
def get_role_permissions():
    """Return the SOP role-permission mapping."""
    return jsonify(ROLE_PERMISSIONS)

@app.route("/api/sop_checklist", methods=["GET"])
def sop_checklist():
    """Return period-end checklist status — shows which steps are completable."""
    today_str = date.today().strftime("%Y-%m")
    with get_conn() as conn:
        # Check ageing report available
        aging_ok = conn.execute(
            "SELECT COUNT(*) AS v FROM invoices WHERE status NOT IN ('Paid','Rejected') AND invoice_month <= ?", (today_str,)
        ).fetchone()["v"] > 0
        # Supplier statements — vendors with outstanding invoices
        outstanding_vendors = conn.execute(
            "SELECT COUNT(DISTINCT vendor_id) AS v FROM invoices WHERE status NOT IN ('Paid','Rejected')"
        ).fetchone()["v"]
        # Payment vouchers — approved batches
        approved_batches = conn.execute(
            "SELECT COUNT(*) AS v FROM payment_batches"
        ).fetchone()["v"]
        # Paid this month
        paid_this_month = conn.execute(
            "SELECT COUNT(*) AS v FROM payments WHERE strftime('%Y-%m',payment_date)=?", (today_str,)
        ).fetchone()["v"]
        # Pending approval backlog
        backlog = conn.execute(
            "SELECT COUNT(*) AS v FROM invoices WHERE status IN ('Draft','Submitted','Verified')"
        ).fetchone()["v"]
        # Rejected this month
        rejected = conn.execute(
            "SELECT COUNT(*) AS v FROM invoices WHERE status='Rejected' AND invoice_month=?", (today_str,)
        ).fetchone()["v"]
        # Tax invoices captured
        tax_inv = conn.execute(
            "SELECT COUNT(*) AS v FROM invoices WHERE invoice_type='Tax Invoice'"
        ).fetchone()["v"]
    return jsonify({
        "month": today_str,
        "checklist": [
            {"step":"SOP-02 Step 1","name":"Creditors Ageing Generated","done":aging_ok,"detail":f"{'Outstanding invoices exist — run Aging report' if aging_ok else 'No outstanding invoices this period'}"},
            {"step":"SOP-02 Step 2","name":"Supplier Statements Requested","done":outstanding_vendors==0,"detail":f"{outstanding_vendors} vendor(s) with outstanding balances"},
            {"step":"SOP-02 Step 3","name":"Creditors Reconciliations Prepared","done":backlog==0,"detail":f"{backlog} invoice(s) still in approval backlog"},
            {"step":"SOP-02 Step 5","name":"Payment Vouchers Prepared","done":approved_batches>0,"detail":f"{approved_batches} payment batch(es) created"},
            {"step":"SOP-02 Step 7","name":"Payment Run Generated","done":paid_this_month>0,"detail":f"{paid_this_month} payment(s) processed this month"},
            {"step":"SOP-01","name":"Tax Invoices Captured","done":tax_inv>0,"detail":f"{tax_inv} tax invoice(s) in system"},
            {"step":"SOP-01","name":"No Rejected Invoices Pending Action","done":rejected==0,"detail":f"{rejected} rejection(s) this month requiring follow-up"},
        ]
    })




# ══════════════════════════════════════════════════════════════════════════════
# MISSING REPORT ROUTES — aging, schedule, rejections
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/reports/aging")
def report_aging():
    """Aging report — groups outstanding invoices by age bucket per vendor."""
    month = request.args.get("month", "")
    sql = """SELECT i.invoice_date, i.outstanding_amount, i.currency,
                    v.name AS vendor, v.payment_method
             FROM invoices i JOIN vendors v ON i.vendor_id = v.vendor_id
             WHERE i.status NOT IN ('Paid', 'Rejected') AND i.outstanding_amount > 0"""
    params = []
    if month:
        sql += " AND i.invoice_month = ?"
        params.append(month)

    BUCKETS = ["0-30 days", "31-60 days", "61-90 days", "91+ days"]

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()

    # Build per-vendor bucket totals
    vendor_data = {}
    totals = {b: 0.0 for b in BUCKETS}

    for r in rows:
        bucket = age_bucket(r["invoice_date"])
        if bucket not in BUCKETS:
            bucket = "91+ days"
        vendor = r["vendor"]
        amt    = float(r["outstanding_amount"] or 0)
        if vendor not in vendor_data:
            vendor_data[vendor] = {b: 0.0 for b in BUCKETS}
            vendor_data[vendor]["total"] = 0.0
            vendor_data[vendor]["vendor"] = vendor
        vendor_data[vendor][bucket] = round(vendor_data[vendor][bucket] + amt, 2)
        vendor_data[vendor]["total"] = round(vendor_data[vendor]["total"] + amt, 2)
        totals[bucket] = round(totals.get(bucket, 0) + amt, 2)

    vendors_list = sorted(vendor_data.values(), key=lambda x: -x["total"])
    grand_total  = round(sum(v["total"] for v in vendors_list), 2)

    return jsonify({
        "vendors":     vendors_list,
        "totals":      totals,
        "grand_total": grand_total,
        "buckets":     BUCKETS,
    })


@app.route("/api/reports/rejections")
def report_rejections():
    """Rejection report grouped by month."""
    month = request.args.get("month", "")
    sql   = """SELECT i.invoice_number, i.total_amount, i.rejection_reason,
                      i.invoice_month, i.invoice_date, i.currency,
                      v.name AS vendor_name,
                      wl.performed_by AS rejected_by,
                      wl.performed_at AS rejected_at
               FROM invoices i
               JOIN vendors v ON i.vendor_id = v.vendor_id
               LEFT JOIN workflow_log wl ON wl.invoice_id = i.invoice_id
                   AND wl.to_status = 'Rejected'
               WHERE i.status = 'Rejected'"""
    params = []
    if month:
        sql += " AND i.invoice_month = ?"
        params.append(month)
    sql += " ORDER BY wl.performed_at DESC"

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()

    from collections import defaultdict
    grouped = defaultdict(list)
    for r in rows:
        key = r["invoice_month"] or "Unknown Month"
        grouped[key].append(dict(r))

    return jsonify({
        "grouped": dict(grouped),
        "total":   len(rows),
    })


@app.route("/api/reports/schedule")
def report_schedule():
    """Payment schedule — all items in payment batches with vendor bank details."""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT pb.batch_reference, pb.scheduled_date,
                   v.name AS vendor, v.payment_method,
                   v.bank_account, v.ecocash_number,
                   i.invoice_number, i.invoice_id, i.outstanding_amount,
                   i.currency, bi.scheduled_amount, bi.item_id
            FROM batch_items bi
            JOIN payment_batches pb ON pb.batch_id = bi.batch_id
            JOIN invoices i         ON i.invoice_id = bi.invoice_id
            JOIN vendors v          ON v.vendor_id  = i.vendor_id
            WHERE i.status NOT IN ('Paid', 'Rejected')
            ORDER BY pb.scheduled_date ASC, v.name ASC
        """).fetchall()
    return jsonify(rows_to_list(rows))

if __name__ == "__main__":
    app.config["SESSION_PERMANENT"] = False
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    init_db()
    app.run(debug=True, port=5000)

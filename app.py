"""
ZCDC Local Vendor Outstanding Payments Tracking System
Flask Web Application
"""

from flask import Flask, request, jsonify, send_from_directory
import sqlite3, uuid, os
from datetime import date, timedelta
from collections import defaultdict

app = Flask(__name__, static_folder="static")
DB_PATH = "zcdc_vendor_payments.db"


# ── DB ────────────────────────────────────────────────────────────────────────

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS vendors (
            vendor_id    TEXT PRIMARY KEY,
            name         TEXT NOT NULL UNIQUE,
            category     TEXT NOT NULL CHECK(category IN ('Services','Supplies','Other')),
            payment_terms INTEGER DEFAULT 30,
            bank_name    TEXT,
            bank_account TEXT,
            created_at   TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS invoices (
            invoice_id        TEXT PRIMARY KEY,
            invoice_number    TEXT NOT NULL,
            vendor_id         TEXT NOT NULL REFERENCES vendors(vendor_id),
            invoice_date      TEXT NOT NULL,
            due_date          TEXT,
            description       TEXT NOT NULL,
            total_amount      REAL NOT NULL CHECK(total_amount > 0),
            outstanding_amount REAL NOT NULL,
            cost_centre       TEXT,
            doc_reference     TEXT,
            status            TEXT NOT NULL DEFAULT 'Draft'
                              CHECK(status IN ('Draft','Submitted','Verified','Rejected',
                                               'Approved','Scheduled','Partially Paid','Paid')),
            rejection_reason  TEXT,
            created_by        TEXT NOT NULL,
            created_at        TEXT DEFAULT (datetime('now')),
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
            scheduled_amount REAL NOT NULL CHECK(scheduled_amount > 0)
        );
        CREATE TABLE IF NOT EXISTS payments (
            payment_id     TEXT PRIMARY KEY,
            invoice_id     TEXT NOT NULL REFERENCES invoices(invoice_id),
            batch_id       TEXT REFERENCES payment_batches(batch_id),
            payment_date   TEXT NOT NULL,
            amount_paid    REAL NOT NULL CHECK(amount_paid > 0),
            bank_reference TEXT,
            recorded_by    TEXT NOT NULL,
            recorded_at    TEXT DEFAULT (datetime('now'))
        );
        """)


def new_id():
    return str(uuid.uuid4())


def log_action(conn, invoice_id, action, from_status, to_status, performed_by, notes=""):
    conn.execute(
        "INSERT INTO workflow_log (log_id,invoice_id,action,from_status,to_status,performed_by,notes) VALUES (?,?,?,?,?,?,?)",
        (new_id(), invoice_id, action, from_status, to_status, performed_by, notes)
    )


def age_bucket(invoice_date_str):
    try:
        age = (date.today() - date.fromisoformat(invoice_date_str)).days
    except Exception:
        return "Unknown"
    if age <= 30:   return "0-30 days"
    if age <= 60:   return "31-60 days"
    if age <= 90:   return "61-90 days"
    return "91+ days"


# ── HELPERS ───────────────────────────────────────────────────────────────────

def row_to_dict(row):
    return dict(row) if row else None


def rows_to_list(rows):
    return [dict(r) for r in rows]


# ── VENDORS ───────────────────────────────────────────────────────────────────

@app.route("/api/vendors", methods=["GET"])
def list_vendors():
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM vendors ORDER BY name").fetchall()
    return jsonify(rows_to_list(rows))


@app.route("/api/vendors", methods=["POST"])
def add_vendor():
    d = request.json
    vid = new_id()
    try:
        with get_conn() as conn:
            conn.execute(
                "INSERT INTO vendors (vendor_id,name,category,payment_terms,bank_name,bank_account) VALUES (?,?,?,?,?,?)",
                (vid, d["name"], d["category"], d.get("payment_terms", 30),
                 d.get("bank_name", ""), d.get("bank_account", ""))
            )
        return jsonify({"ok": True, "vendor_id": vid}), 201
    except sqlite3.IntegrityError as e:
        return jsonify({"error": str(e)}), 400


# ── INVOICES ──────────────────────────────────────────────────────────────────

@app.route("/api/invoices", methods=["GET"])
def list_invoices():
    status = request.args.get("status")
    vendor = request.args.get("vendor_id")
    sql = """SELECT i.*, v.name AS vendor_name
             FROM invoices i JOIN vendors v ON i.vendor_id=v.vendor_id"""
    params = []
    conditions = []
    if status:
        conditions.append("i.status=?"); params.append(status)
    if vendor:
        conditions.append("i.vendor_id=?"); params.append(vendor)
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    sql += " ORDER BY i.invoice_date DESC"
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["age_bucket"] = age_bucket(d["invoice_date"])
        result.append(d)
    return jsonify(result)


@app.route("/api/invoices", methods=["POST"])
def add_invoice():
    d = request.json
    iid = new_id()
    try:
        with get_conn() as conn:
            conn.execute(
                """INSERT INTO invoices
                   (invoice_id,invoice_number,vendor_id,invoice_date,due_date,
                    description,total_amount,outstanding_amount,cost_centre,doc_reference,created_by)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (iid, d["invoice_number"], d["vendor_id"], d["invoice_date"],
                 d.get("due_date"), d["description"], float(d["total_amount"]),
                 float(d["total_amount"]), d.get("cost_centre", ""),
                 d.get("doc_reference", ""), d["created_by"])
            )
            log_action(conn, iid, "Created", None, "Draft", d["created_by"])
        return jsonify({"ok": True, "invoice_id": iid}), 201
    except sqlite3.IntegrityError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/invoices/<iid>", methods=["GET"])
def get_invoice(iid):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT i.*, v.name AS vendor_name FROM invoices i JOIN vendors v ON i.vendor_id=v.vendor_id WHERE i.invoice_id=?",
            (iid,)
        ).fetchone()
        logs = conn.execute(
            "SELECT * FROM workflow_log WHERE invoice_id=? ORDER BY performed_at", (iid,)
        ).fetchall()
    if not row:
        return jsonify({"error": "Not found"}), 404
    d = dict(row)
    d["audit"] = rows_to_list(logs)
    d["age_bucket"] = age_bucket(d["invoice_date"])
    return jsonify(d)


@app.route("/api/invoices/<iid>/workflow", methods=["POST"])
def workflow(iid):
    d = request.json
    action = d.get("action")
    performed_by = d.get("performed_by", "system")
    notes = d.get("notes", "")

    transitions = {
        "submit":  ("Draft",     "Submitted"),
        "verify":  ("Submitted", "Verified"),
        "approve": ("Verified",  "Approved"),
        "reject":  (("Submitted","Verified"), "Rejected"),
    }

    if action not in transitions:
        return jsonify({"error": "Invalid action"}), 400

    with get_conn() as conn:
        inv = conn.execute("SELECT * FROM invoices WHERE invoice_id=?", (iid,)).fetchone()
        if not inv:
            return jsonify({"error": "Invoice not found"}), 404

        allowed_from, to_status = transitions[action]
        if isinstance(allowed_from, str):
            allowed_from = (allowed_from,)

        if inv["status"] not in allowed_from:
            return jsonify({"error": f"Cannot {action}: current status is '{inv['status']}'"}), 400

        update_fields = {"status": to_status}
        if action == "reject":
            update_fields["rejection_reason"] = notes

        conn.execute(
            f"UPDATE invoices SET status=?, rejection_reason=? WHERE invoice_id=?",
            (to_status, update_fields.get("rejection_reason"), iid)
        )
        log_action(conn, iid, action.capitalize(), inv["status"], to_status, performed_by, notes)

    return jsonify({"ok": True, "new_status": to_status})


# ── PAYMENT BATCHES ───────────────────────────────────────────────────────────

@app.route("/api/batches", methods=["GET"])
def list_batches():
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT pb.*, COUNT(bi.item_id) AS invoice_count,
                      COALESCE(SUM(bi.scheduled_amount),0) AS total_scheduled
               FROM payment_batches pb
               LEFT JOIN batch_items bi ON pb.batch_id=bi.batch_id
               GROUP BY pb.batch_id ORDER BY pb.scheduled_date DESC"""
        ).fetchall()
    return jsonify(rows_to_list(rows))


@app.route("/api/batches", methods=["POST"])
def create_batch():
    d = request.json
    bid = new_id()
    try:
        with get_conn() as conn:
            conn.execute(
                "INSERT INTO payment_batches (batch_id,batch_reference,scheduled_date,notes,created_by) VALUES (?,?,?,?,?)",
                (bid, d["batch_reference"], d["scheduled_date"], d.get("notes",""), d["created_by"])
            )
        return jsonify({"ok": True, "batch_id": bid}), 201
    except sqlite3.IntegrityError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/batches/<bid>/items", methods=["POST"])
def add_to_batch(bid):
    d = request.json
    iid = d["invoice_id"]
    amount = float(d["scheduled_amount"])
    added_by = d.get("added_by", "system")

    with get_conn() as conn:
        inv = conn.execute("SELECT * FROM invoices WHERE invoice_id=?", (iid,)).fetchone()
        if not inv:
            return jsonify({"error": "Invoice not found"}), 404
        if inv["status"] != "Approved":
            return jsonify({"error": f"Invoice must be Approved (current: {inv['status']})"}), 400
        if amount > inv["outstanding_amount"]:
            return jsonify({"error": "Scheduled amount exceeds outstanding balance"}), 400

        conn.execute(
            "INSERT INTO batch_items (item_id,batch_id,invoice_id,scheduled_amount) VALUES (?,?,?,?)",
            (new_id(), bid, iid, amount)
        )
        conn.execute("UPDATE invoices SET status='Scheduled' WHERE invoice_id=?", (iid,))
        log_action(conn, iid, "Scheduled", "Approved", "Scheduled", added_by,
                   f"Batch {bid[:8]}, Amount: {amount}")

    return jsonify({"ok": True})


# ── PAYMENTS ──────────────────────────────────────────────────────────────────

@app.route("/api/payments", methods=["POST"])
def record_payment():
    d = request.json
    iid = d["invoice_id"]
    amount = float(d["amount_paid"])

    with get_conn() as conn:
        inv = conn.execute("SELECT * FROM invoices WHERE invoice_id=?", (iid,)).fetchone()
        if not inv:
            return jsonify({"error": "Invoice not found"}), 404
        if inv["status"] not in ("Approved","Scheduled","Partially Paid"):
            return jsonify({"error": f"Cannot pay invoice with status '{inv['status']}'"}), 400
        if amount > inv["outstanding_amount"]:
            return jsonify({"error": "Payment exceeds outstanding balance"}), 400

        new_outstanding = round(inv["outstanding_amount"] - amount, 2)
        new_status = "Paid" if new_outstanding == 0 else "Partially Paid"

        conn.execute(
            "INSERT INTO payments (payment_id,invoice_id,batch_id,payment_date,amount_paid,bank_reference,recorded_by) VALUES (?,?,?,?,?,?,?)",
            (new_id(), iid, d.get("batch_id"), d["payment_date"], amount,
             d.get("bank_reference",""), d["recorded_by"])
        )
        conn.execute(
            "UPDATE invoices SET outstanding_amount=?, status=? WHERE invoice_id=?",
            (new_outstanding, new_status, iid)
        )
        log_action(conn, iid, f"Payment ({new_status})", inv["status"], new_status,
                   d["recorded_by"], f"Paid: {amount}, Ref: {d.get('bank_reference','')}")

    return jsonify({"ok": True, "new_status": new_status, "outstanding": new_outstanding})


# ── REPORTS ───────────────────────────────────────────────────────────────────

@app.route("/api/reports/outstanding", methods=["GET"])
def report_outstanding():
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT v.name AS vendor, v.category,
                      COUNT(i.invoice_id) AS invoice_count,
                      COALESCE(SUM(i.outstanding_amount),0) AS total_outstanding
               FROM invoices i JOIN vendors v ON i.vendor_id=v.vendor_id
               WHERE i.status NOT IN ('Paid','Rejected')
               GROUP BY v.vendor_id ORDER BY total_outstanding DESC"""
        ).fetchall()
    return jsonify(rows_to_list(rows))


@app.route("/api/reports/aging", methods=["GET"])
def report_aging():
    buckets = ["0-30 days", "31-60 days", "61-90 days", "91+ days"]
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT v.name AS vendor, i.invoice_date, i.outstanding_amount
               FROM invoices i JOIN vendors v ON i.vendor_id=v.vendor_id
               WHERE i.status NOT IN ('Paid','Rejected')"""
        ).fetchall()

    vendor_data = defaultdict(lambda: defaultdict(float))
    totals = defaultdict(float)
    for r in rows:
        b = age_bucket(r["invoice_date"])
        vendor_data[r["vendor"]][b] += r["outstanding_amount"]
        totals[b] += r["outstanding_amount"]

    result = []
    for vendor, bdata in vendor_data.items():
        row = {"vendor": vendor, "total": sum(bdata.values())}
        for b in buckets:
            row[b] = bdata.get(b, 0)
        result.append(row)
    result.sort(key=lambda x: -x["total"])

    return jsonify({
        "vendors": result,
        "totals": {b: totals.get(b, 0) for b in buckets},
        "grand_total": sum(totals.values())
    })


@app.route("/api/reports/backlog", methods=["GET"])
def report_backlog():
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT i.*, v.name AS vendor_name
               FROM invoices i JOIN vendors v ON i.vendor_id=v.vendor_id
               WHERE i.status IN ('Draft','Submitted','Verified')
               ORDER BY i.invoice_date"""
        ).fetchall()
    return jsonify(rows_to_list(rows))


@app.route("/api/reports/schedule", methods=["GET"])
def report_schedule():
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT pb.batch_reference, pb.scheduled_date,
                      v.name AS vendor, i.invoice_number,
                      bi.scheduled_amount, i.invoice_id
               FROM batch_items bi
               JOIN payment_batches pb ON bi.batch_id=pb.batch_id
               JOIN invoices i ON bi.invoice_id=i.invoice_id
               JOIN vendors v ON i.vendor_id=v.vendor_id
               WHERE i.status='Scheduled'
               ORDER BY pb.scheduled_date, v.name"""
        ).fetchall()
    return jsonify(rows_to_list(rows))


@app.route("/api/reports/dashboard", methods=["GET"])
def dashboard():
    with get_conn() as conn:
        stats = {}
        stats["total_outstanding"] = conn.execute(
            "SELECT COALESCE(SUM(outstanding_amount),0) AS v FROM invoices WHERE status NOT IN ('Paid','Rejected')"
        ).fetchone()["v"]
        stats["pending_approval"] = conn.execute(
            "SELECT COUNT(*) AS v FROM invoices WHERE status IN ('Draft','Submitted','Verified')"
        ).fetchone()["v"]
        stats["approved_unscheduled"] = conn.execute(
            "SELECT COUNT(*) AS v FROM invoices WHERE status='Approved'"
        ).fetchone()["v"]
        stats["scheduled_total"] = conn.execute(
            "SELECT COALESCE(SUM(outstanding_amount),0) AS v FROM invoices WHERE status='Scheduled'"
        ).fetchone()["v"]
        stats["overdue_91plus"] = conn.execute(
            "SELECT COUNT(*) AS v FROM invoices WHERE status NOT IN ('Paid','Rejected') AND invoice_date <= date('now','-91 days')"
        ).fetchone()["v"]
        stats["paid_this_month"] = conn.execute(
            "SELECT COALESCE(SUM(amount_paid),0) AS v FROM payments WHERE strftime('%Y-%m',payment_date)=strftime('%Y-%m','now')"
        ).fetchone()["v"]
    return jsonify(stats)


# ── STATIC ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


if __name__ == "__main__":
    init_db()
    # Seed sample data if empty
    with get_conn() as conn:
        if not conn.execute("SELECT 1 FROM vendors LIMIT 1").fetchone():
            from datetime import date, timedelta
            today = date.today()
            v1 = new_id(); v2 = new_id(); v3 = new_id()
            conn.execute("INSERT INTO vendors VALUES (?,?,?,?,?,?,datetime('now'))",
                         (v1,"ABC Electrical Supplies","Supplies",30,"CABS","1234567"))
            conn.execute("INSERT INTO vendors VALUES (?,?,?,?,?,?,datetime('now'))",
                         (v2,"XYZ Maintenance Services","Services",45,"ZB","7654321"))
            conn.execute("INSERT INTO vendors VALUES (?,?,?,?,?,?,datetime('now'))",
                         (v3,"Rapid Logistics Ltd","Services",14,"",""))

            def make_inv(inv_no, vid, days_ago, desc, amount, status, cc="Mining-Ops"):
                iid = new_id()
                inv_date = (today - timedelta(days=days_ago)).isoformat()
                outstanding = amount if status not in ("Paid",) else 0
                if status == "Partially Paid": outstanding = round(amount * 0.4, 2)
                conn.execute(
                    "INSERT INTO invoices (invoice_id,invoice_number,vendor_id,invoice_date,description,total_amount,outstanding_amount,cost_centre,status,created_by) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (iid,inv_no,vid,inv_date,desc,amount,outstanding,cc,status,"clerk_jane")
                )
                conn.execute("INSERT INTO workflow_log VALUES (?,?,?,?,?,?,?,datetime('now'))",
                             (new_id(),iid,"Created",None,"Draft","clerk_jane",""))
                return iid

            make_inv("INV-2024-001",v1,98,"Electrical cables and fittings",15000,"Approved")
            make_inv("INV-2024-002",v2,50,"Monthly maintenance contract",8500,"Verified")
            make_inv("INV-2024-003",v3,10,"Fuel delivery transport",3200,"Submitted")
            make_inv("INV-2024-004",v1,70,"Copper wire batch B",22000,"Partially Paid","Plant-Maint")
            make_inv("INV-2024-005",v2,5,"Office cleaning services",1200,"Draft","Admin")
            make_inv("INV-2024-006",v3,35,"Cold storage logistics",4800,"Approved","Logistics")

    app.run(debug=True, port=5000)

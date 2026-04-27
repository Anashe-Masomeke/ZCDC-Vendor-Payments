"""
email_service.py - Real Email Delivery for ZCDC Vendor Tracking System
=======================================================================
Supports: Gmail SMTP  |  SendGrid API

HOW TO SET UP GMAIL (free, easiest to start):
1. Go to myaccount.google.com
2. Security → 2-Step Verification → turn ON
3. Security → App passwords → create one called "ZCDC System"
4. Copy the 16-character password it gives you
5. Set EMAIL_ADDRESS = your Gmail address below
6. Set EMAIL_APP_PASSWORD = that 16-character password
7. Set EMAIL_ENABLED = True

HOW TO SET UP SENDGRID (professional, 100 free emails/day):
1. Sign up free at sendgrid.com
2. Settings → API Keys → Create API Key (Full Access)
3. Settings → Sender Authentication → verify your sender email
4. Set EMAIL_PROVIDER = "sendgrid"
5. Set SENDGRID_API_KEY = your key
6. Set EMAIL_ENABLED = True
"""

import smtplib, os, logging, sqlite3
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════
# ▶▶  EDIT THESE SETTINGS  ◀◀
# ══════════════════════════════════════════════════════════════════

EMAIL_ENABLED      = True                       # Set True to send real emails
EMAIL_PROVIDER     = "sendgrid"                     # "gmail" or "sendgrid"

# Gmail
EMAIL_ADDRESS      = "masomekeanashe4@gmail.com"      # Your Gmail address
EMAIL_APP_PASSWORD = "lcny tmqu wjjn emxr"       # 16-char App Password from Google

# SendGrid (only needed if EMAIL_PROVIDER = "sendgrid")
SENDGRID_API_KEY   = "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"

# Company details
COMPANY_NAME       = "ZCDC"
COMPANY_FULL_NAME  = "Zimbabwe Consolidated Diamond Company"
COMPANY_EMAIL      = "masomekeanashe4@gmail.com"      # Shown as reply-to address

# ── Internal staff emails — set each person's real email address ──────────────
# These are the actual addresses that receive internal notification emails.
ROLE_EMAILS = {
    "Finance Manager":                          "masomekeanashe4@gmail.com",
    "Chief Finance Officer":                    "cfo@zcdc.co.zw",
    "Assistant Finance Manager":                "afm@zcdc.co.zw",
    "Cost and Management Accountant":           "cma@zcdc.co.zw",
    "Assistant Cost and Management Accountant": "acma@zcdc.co.zw",
    "Cost and Management Clerk":                "clerk@zcdc.co.zw",
    "Receiving Clerk":                          "receiving@zcdc.co.zw",
    "Treasury Officer":                         "treasury@zcdc.co.zw",
}

# DB path — must match app.py DB_PATH
_BASE   = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(_BASE, "zcdc_vendor_payments.db")

GOLD  = "#C8960C"
BLACK = "#111111"


# ══════════════════════════════════════════════════════════════════
# CORE SEND
# ══════════════════════════════════════════════════════════════════

def send_email(to_email: str, subject: str, html_body: str,
               plain_body: str = "", attachment_path: str = None) -> dict:
    if not to_email or "@" not in to_email:
        return {"success": False, "message": f"Invalid email: '{to_email}'"}

    if not EMAIL_ENABLED:
        print(f"\n{'='*60}")
        print(f"📧 EMAIL PREVIEW (not sent — set EMAIL_ENABLED=True to send for real)")
        print(f"   To:      {to_email}")
        print(f"   Subject: {subject}")
        print(f"   Body:    {plain_body[:150]}...")
        print(f"{'='*60}\n")
        _log("SIMULATED", to_email, subject, "EMAIL_ENABLED=False")
        return {"success": True, "message": f"Simulated to {to_email}"}

    if EMAIL_PROVIDER == "sendgrid":
        return _sendgrid(to_email, subject, html_body, plain_body, attachment_path)
    return _gmail(to_email, subject, html_body, plain_body, attachment_path)


def send_to_role(role: str, subject: str, html: str, plain: str = "") -> dict:
    """Send to the internal email address registered for a role."""
    email = ROLE_EMAILS.get(role)
    if not email:
        logger.warning("No email set for role: %s", role)
        return {"success": False, "message": f"No email for role: {role}"}
    return send_email(email, subject, html, plain)


def send_to_roles(roles: list, subject: str, html: str, plain: str = "") -> list:
    """Send to multiple roles (deduplicates addresses)."""
    seen, results = set(), []
    for role in roles:
        addr = ROLE_EMAILS.get(role)
        if addr and addr not in seen:
            results.append(send_email(addr, subject, html, plain))
            seen.add(addr)
    return results


# ══════════════════════════════════════════════════════════════════
# GMAIL
# ══════════════════════════════════════════════════════════════════

def _gmail(to_email, subject, html_body, plain_body, attachment_path):
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = f"{COMPANY_NAME} Finance <{EMAIL_ADDRESS}>"
        msg["To"]      = to_email
        msg["Reply-To"]= COMPANY_EMAIL
        if plain_body:
            msg.attach(MIMEText(plain_body, "plain", "utf-8"))
        msg.attach(MIMEText(html_body, "html", "utf-8"))
        if attachment_path and os.path.exists(attachment_path):
            with open(attachment_path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition",
                            f"attachment; filename={os.path.basename(attachment_path)}")
            msg.attach(part)
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as s:
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.login(EMAIL_ADDRESS, EMAIL_APP_PASSWORD)
            s.send_message(msg)
        _log("SENT", to_email, subject, "Gmail")
        return {"success": True, "message": f"Sent to {to_email}"}
    except smtplib.SMTPAuthenticationError:
        m = "Gmail auth failed. Check EMAIL_ADDRESS and EMAIL_APP_PASSWORD."
        _log("AUTH_FAIL", to_email, subject, m)
        return {"success": False, "message": m}
    except Exception as e:
        _log("ERROR", to_email, subject, str(e))
        return {"success": False, "message": str(e)}


# ══════════════════════════════════════════════════════════════════
# SENDGRID
# ══════════════════════════════════════════════════════════════════

def _sendgrid(to_email, subject, html_body, plain_body, attachment_path):
    try:
        import urllib.request, json, base64 as b64
        data = {
            "personalizations": [{"to": [{"email": to_email}]}],
            "from": {"email": EMAIL_ADDRESS, "name": f"{COMPANY_NAME} Finance"},
            "reply_to": {"email": EMAIL_ADDRESS},
            "subject": subject,
            "content": [
                {"type": "text/plain", "value": plain_body or "View in HTML client."},
                {"type": "text/html", "value": html_body}
            ],
            "headers": {
                "X-Priority": "1",
                "Importance": "high"
            },
            "mail_settings": {
                "bypass_spam_management": {
                    "enable": True
                }
            },
            "tracking_settings": {
                "click_tracking": {"enable": False},
                "open_tracking": {"enable": False}
            }
        }
        if attachment_path and os.path.exists(attachment_path):
            with open(attachment_path, "rb") as f:
                enc = b64.b64encode(f.read()).decode()
            data["attachments"] = [{
                "content": enc, "filename": os.path.basename(attachment_path),
                "type": "text/csv", "disposition": "attachment"
            }]
        req = urllib.request.Request(
            "https://api.sendgrid.com/v3/mail/send",
            data=json.dumps(data).encode("utf-8"),
            headers={"Authorization": f"Bearer {SENDGRID_API_KEY}",
                     "Content-Type": "application/json"},
            method="POST"
        )
        urllib.request.urlopen(req, timeout=15)
        _log("SENT", to_email, subject, "SendGrid")
        return {"success": True, "message": f"Sent via SendGrid to {to_email}"}
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:200]
        _log(f"HTTP_{e.code}", to_email, subject, body)
        return {"success": False, "message": f"SendGrid {e.code}: {body}"}
    except Exception as e:
        _log("ERROR", to_email, subject, str(e))
        return {"success": False, "message": str(e)}


# ══════════════════════════════════════════════════════════════════
# EMAIL LOG
# ══════════════════════════════════════════════════════════════════

def _log(status, to_email, subject, detail=""):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""CREATE TABLE IF NOT EXISTS email_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status TEXT, to_email TEXT, subject TEXT,
            detail TEXT, sent_at TEXT DEFAULT (datetime('now')))""")
        conn.execute("INSERT INTO email_log (status,to_email,subject,detail) VALUES (?,?,?,?)",
                     (status, to_email, subject[:250], str(detail)[:500]))
        conn.commit()
        conn.close()
    except Exception:
        pass

def get_email_log(limit=100):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM email_log ORDER BY sent_at DESC LIMIT ?", (limit,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


# ══════════════════════════════════════════════════════════════════
# HTML BASE TEMPLATE
# ══════════════════════════════════════════════════════════════════

def _tmpl(title: str, body: str, is_vendor=False) -> str:
    footer = "Automated message from ZCDC Vendor Tracking System. Do not reply."
    if is_vendor:
        footer += f" Queries: {COMPANY_EMAIL}"
    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<style>
 body{{margin:0;padding:20px;background:#f4f4f4;
       font-family:'Segoe UI',Arial,sans-serif;font-size:14px;color:#222}}
 .w{{max-width:620px;margin:0 auto;background:#fff;border-radius:12px;
      overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,.1)}}
 .h{{background:{BLACK};padding:28px 32px}}
 .hm{{font-size:10px;font-weight:700;letter-spacing:.2em;color:{GOLD};
      text-transform:uppercase;margin-bottom:6px}}
 .ht{{font-size:20px;font-weight:700;color:#fff}}
 .b{{padding:28px 32px;line-height:1.8}}
 .amt{{font-size:24px;font-weight:700;color:{GOLD};font-family:monospace;
       padding:8px 14px;background:#fafaf0;border-left:4px solid {GOLD};
       border-radius:0 6px 6px 0;display:inline-block;margin:10px 0}}
 table{{width:100%;border-collapse:collapse;margin:14px 0;font-size:13px}}
 th{{background:#f8f8f0;padding:8px 12px;text-align:left;border-bottom:2px solid #e8e0c0;
     font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#888}}
 td{{padding:9px 12px;border-bottom:1px solid #f0f0f0;vertical-align:top}}
 .badge{{display:inline-block;padding:3px 9px;border-radius:9px;font-size:11px;
         font-weight:700;background:#fef3c7;color:#92400e}}
 .g{{background:#d1fae5;color:#065f46}}.r{{background:#fee2e2;color:#991b1b}}
 .bl{{background:#dbeafe;color:#1e40af}}
 .alert{{padding:13px 15px;border-radius:8px;margin:14px 0;font-size:13px;line-height:1.7}}
 .aw{{background:#fef3c7;border:1px solid #fcd34d;color:#92400e}}
 .ar{{background:#fee2e2;border:1px solid #fca5a5;color:#991b1b}}
 .ag{{background:#d1fae5;border:1px solid #6ee7b7;color:#065f46}}
 hr{{border:none;border-top:1px solid #f0f0f0;margin:18px 0}}
 .ft{{background:#f8f8f8;padding:14px 32px;font-size:11px;
      color:#999;border-top:1px solid #eee;line-height:1.7}}
</style></head><body><div class="w">
<div class="h"><div class="hm">{COMPANY_FULL_NAME}</div>
<div class="ht">{title}</div></div>
<div class="b">{body}</div>
<div class="ft">{footer}<br>
© {datetime.now().year} {COMPANY_FULL_NAME}</div>
</div></body></html>"""


# ══════════════════════════════════════════════════════════════════
# EMAIL TEMPLATES — called from app.py at each workflow step
# ══════════════════════════════════════════════════════════════════

class Emails:

    # 1. INVOICE SUBMITTED → notify Finance Manager to verify
    @staticmethod
    def invoice_submitted(invoice_number, vendor_name, amount_usd,
                           invoice_date, description, submitted_by, cost_centre=""):
        subject = f"[Action Required] Invoice Submitted — {invoice_number}"
        plain   = (f"Invoice {invoice_number} from {vendor_name} "
                   f"(USD {amount_usd:,.2f}) submitted by {submitted_by}. "
                   f"Please log in to verify.")
        html = _tmpl("Invoice Submitted — Verification Required", f"""
          <p>A new invoice has been submitted and requires your verification:</p>
          <table><tr><th>Invoice #</th><td><strong>{invoice_number}</strong></td></tr>
          <tr><th>Vendor</th><td>{vendor_name}</td></tr>
          <tr><th>Amount</th><td><div class="amt">USD {amount_usd:,.2f}</div></td></tr>
          <tr><th>Invoice Date</th><td>{invoice_date}</td></tr>
          <tr><th>Description</th><td>{description}</td></tr>
          {'<tr><th>Cost Centre</th><td>'+cost_centre+'</td></tr>' if cost_centre else ''}
          <tr><th>Submitted By</th><td>{submitted_by}</td></tr>
          <tr><th>Status</th><td><span class="badge bl">Submitted — Awaiting Verification</span></td></tr>
          </table>
          <p>Per <strong>IMS-FIN-SOP-01</strong>, please log in to verify this invoice.</p>
        """)
        send_to_roles(["Finance Manager", "Assistant Finance Manager"], subject, html, plain)

    # 2. INVOICE VERIFIED → notify FM/CFO to approve
    @staticmethod
    def invoice_verified(invoice_number, vendor_name, amount_usd,
                          verified_by, cost_centre=""):
        subject = f"[Action Required] Invoice Verified — {invoice_number}"
        plain   = (f"Invoice {invoice_number} ({vendor_name}, USD {amount_usd:,.2f}) "
                   f"verified by {verified_by}. Awaiting your approval.")
        html = _tmpl("Invoice Verified — Approval Required", f"""
          <p>The following invoice has been verified and requires your approval:</p>
          <table><tr><th>Invoice #</th><td><strong>{invoice_number}</strong></td></tr>
          <tr><th>Vendor</th><td>{vendor_name}</td></tr>
          <tr><th>Amount</th><td><div class="amt">USD {amount_usd:,.2f}</div></td></tr>
          {'<tr><th>Cost Centre</th><td>'+cost_centre+'</td></tr>' if cost_centre else ''}
          <tr><th>Verified By</th><td>{verified_by}</td></tr>
          <tr><th>Status</th><td><span class="badge">Verified — Pending Approval</span></td></tr>
          </table>
          <p>Per <strong>IMS-FIN-SOP-01</strong>, please approve or reject this invoice.</p>
        """)
        send_to_roles(["Finance Manager", "Chief Finance Officer"], subject, html, plain)

    # 3. INVOICE APPROVED → vendor email + treasury alert
    @staticmethod
    def invoice_approved(vendor_email, vendor_name, invoice_number,
                          amount_usd, approved_by, expected_payment_date=""):
        # --- To vendor ---
        subject_v = f"Invoice Approved — {invoice_number} — {COMPANY_NAME}"
        plain_v   = (f"Dear {vendor_name}, your invoice {invoice_number} "
                     f"for USD {amount_usd:,.2f} has been approved. "
                     f"{'Expected payment: '+expected_payment_date+'.' if expected_payment_date else ''}")
        html_v = _tmpl("Your Invoice Has Been Approved ✓", f"""
          <p>Dear <strong>{vendor_name}</strong>,</p>
          <p>We are pleased to confirm that your invoice has been
          <strong style="color:#065f46">approved</strong> for payment:</p>
          <table><tr><th>Invoice #</th><td><strong>{invoice_number}</strong></td></tr>
          <tr><th>Amount Approved</th><td><div class="amt">USD {amount_usd:,.2f}</div></td></tr>
          <tr><th>Approved By</th><td>{approved_by}</td></tr>
          {'<tr><th>Expected Payment</th><td><strong>'+expected_payment_date+'</strong></td></tr>' if expected_payment_date else ''}
          </table>
          <div class="alert ag">Your invoice is now in our payment queue.
          You will receive a payment confirmation once funds are sent.</div>
          <p>Queries: <strong>{COMPANY_EMAIL}</strong></p>
        """, is_vendor=True)
        if vendor_email:
            send_email(vendor_email, subject_v, html_v, plain_v)

        # --- To Treasury Officer ---
        subject_t = f"[Action Required] Approved Invoice — Schedule Payment — {invoice_number}"
        plain_t   = (f"Invoice {invoice_number} ({vendor_name}, USD {amount_usd:,.2f}) "
                     f"approved. Please add to a payment batch.")
        html_t = _tmpl("Invoice Approved — Schedule Payment", f"""
          <p>The following invoice has been approved and must be added to a payment batch:</p>
          <table><tr><th>Invoice #</th><td><strong>{invoice_number}</strong></td></tr>
          <tr><th>Vendor</th><td>{vendor_name}</td></tr>
          <tr><th>Amount</th><td><div class="amt">USD {amount_usd:,.2f}</div></td></tr>
          <tr><th>Approved By</th><td>{approved_by}</td></tr>
          </table>
          <p>Per <strong>IMS-FIN-SOP-01</strong>, please create or add to a payment batch.</p>
        """)
        send_to_roles(["Treasury Officer", "Finance Manager"], subject_t, html_t, plain_t)

    # 4. INVOICE REJECTED → vendor + internal
    @staticmethod
    def invoice_rejected(vendor_email, vendor_name, invoice_number,
                          amount_usd, rejection_reason, rejected_by):
        # --- To vendor ---
        subject_v = f"Invoice Returned for Correction — {invoice_number} — {COMPANY_NAME}"
        plain_v   = (f"Dear {vendor_name}, your invoice {invoice_number} "
                     f"(USD {amount_usd:,.2f}) has been returned.\n"
                     f"Reason: {rejection_reason}\n"
                     f"Please correct and resubmit to {COMPANY_EMAIL}.")
        html_v = _tmpl("Invoice Returned for Correction", f"""
          <p>Dear <strong>{vendor_name}</strong>,</p>
          <p>Your invoice has been returned and requires correction:</p>
          <table><tr><th>Invoice #</th><td><strong>{invoice_number}</strong></td></tr>
          <tr><th>Amount</th><td>USD {amount_usd:,.2f}</td></tr>
          <tr><th>Returned By</th><td>{rejected_by}</td></tr>
          </table>
          <div class="alert ar"><strong>Reason:</strong><br>{rejection_reason}</div>
          <p>Please correct the issues and resubmit to <strong>{COMPANY_EMAIL}</strong>.</p>
        """, is_vendor=True)
        if vendor_email:
            send_email(vendor_email, subject_v, html_v, plain_v)

        # --- Internal ---
        subject_i = f"Invoice Rejected — {invoice_number} — {vendor_name}"
        plain_i   = f"Invoice {invoice_number} rejected by {rejected_by}. Reason: {rejection_reason}"
        html_i = _tmpl("Invoice Rejection Notice", f"""
          <p>Invoice rejected and vendor notified:</p>
          <table><tr><th>Invoice #</th><td><strong>{invoice_number}</strong></td></tr>
          <tr><th>Vendor</th><td>{vendor_name}</td></tr>
          <tr><th>Amount</th><td>USD {amount_usd:,.2f}</td></tr>
          <tr><th>Rejected By</th><td>{rejected_by}</td></tr>
          <tr><th>Reason</th><td style="color:#991b1b">{rejection_reason}</td></tr>
          </table>
        """)
        send_to_roles(["Finance Manager", "Cost and Management Clerk"], subject_i, html_i, plain_i)

    # 5. PAYMENT BATCH CREATED → treasury + each vendor
    @staticmethod
    def batch_created(batch_reference, scheduled_date, total_amount,
                       invoice_summaries: list, created_by):
        """
        invoice_summaries = [
          {"vendor_name","vendor_email","invoice_number","amount"}, ...
        ]
        """
        # --- Internal to Treasury + FM ---
        rows = "".join(
            f"<tr><td>{s['vendor_name']}</td>"
            f"<td style='font-family:monospace'>{s['invoice_number']}</td>"
            f"<td style='text-align:right'>USD {s['amount']:,.2f}</td></tr>"
            for s in invoice_summaries
        )
        subject_i = f"Payment Batch Created — {batch_reference} — USD {total_amount:,.2f}"
        plain_i   = (f"Batch {batch_reference} created for {scheduled_date}. "
                     f"Total: USD {total_amount:,.2f}. {len(invoice_summaries)} invoices.")
        html_i = _tmpl("Payment Batch Created — Action Required", f"""
          <p>A payment batch has been created and requires processing:</p>
          <table><tr><th>Batch Ref</th><td><strong>{batch_reference}</strong></td></tr>
          <tr><th>Scheduled Date</th>
              <td style="color:{GOLD};font-weight:700;font-size:16px">{scheduled_date}</td></tr>
          <tr><th>Total Amount</th><td><div class="amt">USD {total_amount:,.2f}</div></td></tr>
          <tr><th>Invoices</th><td>{len(invoice_summaries)}</td></tr>
          <tr><th>Created By</th><td>{created_by}</td></tr>
          </table>
          <p>Invoices included:</p>
          <table><thead><tr><th>Vendor</th><th>Invoice #</th>
            <th style="text-align:right">Amount</th></tr></thead>
          <tbody>{rows}</tbody>
          <tfoot><tr style="background:#f8f8f0">
            <td colspan="2"><strong>TOTAL</strong></td>
            <td style="text-align:right;font-weight:700">USD {total_amount:,.2f}</td>
          </tr></tfoot></table>
          <p>Per <strong>IMS-FIN-SOP-01</strong>, process payments on or before
          the scheduled date and record each payment in the system.</p>
        """)
        send_to_roles(["Treasury Officer", "Finance Manager"], subject_i, html_i, plain_i)

        # --- Email each vendor their portion ---
        vendor_map = defaultdict(list)
        for s in invoice_summaries:
            if s.get("vendor_email"):
                vendor_map[s["vendor_email"]].append(s)

        for vemail, vitems in vendor_map.items():
            vname  = vitems[0]["vendor_name"]
            vtotal = sum(v["amount"] for v in vitems)
            vrows  = "".join(
                f"<tr><td style='font-family:monospace'>{v['invoice_number']}</td>"
                f"<td style='text-align:right;font-weight:600'>USD {v['amount']:,.2f}</td></tr>"
                for v in vitems
            )
            subject_v = f"Payment Scheduled — {batch_reference} — {COMPANY_NAME}"
            plain_v   = (f"Dear {vname}, payment of USD {vtotal:,.2f} is scheduled "
                         f"for {scheduled_date}. Ref: {batch_reference}.")
            html_v = _tmpl("Your Payment Has Been Scheduled", f"""
              <p>Dear <strong>{vname}</strong>,</p>
              <p>Payment for the following invoice(s) has been scheduled:</p>
              <table><tr><th>Batch Ref</th><td><strong>{batch_reference}</strong></td></tr>
              <tr><th>Payment Date</th>
                  <td style="color:{GOLD};font-weight:700;font-size:16px">{scheduled_date}</td></tr>
              <tr><th>Total Amount</th><td><div class="amt">USD {vtotal:,.2f}</div></td></tr>
              </table>
              <table><thead><tr><th>Invoice #</th>
                <th style="text-align:right">Amount</th></tr></thead>
              <tbody>{vrows}</tbody></table>
              <div class="alert ag">Please ensure your bank account details are correct.
              Contact <strong>{COMPANY_EMAIL}</strong> immediately if there are discrepancies.</div>
            """, is_vendor=True)
            send_email(vemail, subject_v, html_v, plain_v)

    # 6. PAYMENT RECORDED → vendor confirmation
    @staticmethod
    def payment_confirmed(vendor_email, vendor_name, invoice_number,
                           amount_usd, payment_date, payment_method,
                           bank_reference, recorded_by):
        subject = f"Payment Confirmation — {invoice_number} — {COMPANY_NAME}"
        plain   = (f"Dear {vendor_name}, payment of USD {amount_usd:,.2f} "
                   f"for invoice {invoice_number} processed on {payment_date}. "
                   f"Method: {payment_method}. Ref: {bank_reference or 'N/A'}.")
        html = _tmpl("Payment Confirmation", f"""
          <p>Dear <strong>{vendor_name}</strong>,</p>
          <p>We confirm the following payment has been processed:</p>
          <div class="amt">USD {amount_usd:,.2f}</div>
          <table><tr><th>Invoice #</th><td><strong>{invoice_number}</strong></td></tr>
          <tr><th>Payment Date</th><td>{payment_date}</td></tr>
          <tr><th>Payment Method</th><td>{payment_method}</td></tr>
          <tr><th>Reference</th><td><strong>{bank_reference or 'See bank statement'}</strong></td></tr>
          <tr><th>Processed By</th><td>{recorded_by}</td></tr>
          </table>
          <div class="alert ag">Allow <strong>1–2 business days</strong> for funds to reflect.
          If not received within 3 days, contact <strong>{COMPANY_EMAIL}</strong>
          quoting the reference above.</div>
          <p>Thank you for your continued partnership with {COMPANY_NAME}.</p>
        """, is_vendor=True)
        if vendor_email:
            send_email(vendor_email, subject, html, plain)

    # 7. OVERDUE ALERT → FM + CFO (call daily or weekly)
    @staticmethod
    def overdue_alert(overdue_items: list, total_overdue: float):
        if not overdue_items:
            return
        rows = "".join(
            f"<tr><td>{i['vendor']}</td>"
            f"<td style='font-family:monospace'>{i['invoice_number']}</td>"
            f"<td style='color:#dc2626;font-weight:600'>{i['age_days']}d</td>"
            f"<td style='text-align:right;font-weight:600'>USD {i['outstanding']:,.2f}</td></tr>"
            for i in overdue_items[:25]
        )
        subject = (f"⚠️ Overdue Invoices Alert — {len(overdue_items)} items — "
                   f"USD {total_overdue:,.2f}")
        plain   = (f"{len(overdue_items)} overdue invoices totalling "
                   f"USD {total_overdue:,.2f}. Please log in to action.")
        html = _tmpl("⚠️ Overdue Invoices — Action Required", f"""
          <div class="alert aw">
            <strong>{len(overdue_items)} overdue invoices</strong> totalling
            <strong>USD {total_overdue:,.2f}</strong> require immediate attention.
          </div>
          <table><thead><tr><th>Vendor</th><th>Invoice #</th>
            <th>Age</th><th style="text-align:right">Outstanding</th></tr></thead>
          <tbody>{rows}</tbody></table>
          {'<p style="font-size:12px;color:#999">Top 25 shown. See system for full list.</p>' if len(overdue_items)>25 else ''}
          <p>Log in to the ZCDC Vendor Tracking System to action these per IMS-FIN-SOP-01.</p>
        """)
        send_to_roles(
            ["Finance Manager", "Chief Finance Officer", "Assistant Finance Manager"],
            subject, html, plain
        )

    # 8. NEW VENDOR ADDED → FM notification
    @staticmethod
    def vendor_added(vendor_name, category, payment_method,
                      bank_account, added_by):
        subject = f"New Vendor Registered — {vendor_name}"
        plain   = f"New vendor added: {vendor_name} ({category}). Added by: {added_by}."
        html = _tmpl("New Vendor Registered", f"""
          <p>A new vendor has been registered:</p>
          <table><tr><th>Vendor Name</th><td><strong>{vendor_name}</strong></td></tr>
          <tr><th>Category</th><td>{category}</td></tr>
          <tr><th>Payment Method</th><td>{payment_method}</td></tr>
          <tr><th>Bank Account</th><td>{bank_account or '—'}</td></tr>
          <tr><th>Added By</th><td>{added_by}</td></tr>
          </table>
          <p>Please verify vendor details in the system.</p>
        """)
        send_to_roles(["Finance Manager"], subject, html, plain)

"""
hourly_alert_no_email.py
------------------------
Vercel serverless handler: /api/hourly_alert_no_email

Reuses fetch_all() and evaluate_alerts() from hourly_alert.py.
Prices are fetched LIVE at the moment the HTTP request arrives —
no scheduling, no caching, no email sent.

Returns a rich JSON response for the frontend dashboard.
"""

import datetime
import logging
import json
import os
import pytz
from http.server import BaseHTTPRequestHandler
from dotenv import load_dotenv

# ── Reuse all data-fetching logic from hourly_alert ───────────────────────────
from hourly_alert import (
    fetch_all,
    evaluate_alerts,
    HOURLY_PRICE_CHANGE_THRESHOLD,
    TIMEZONE,
)

# ── Load env (same .env as the rest of the project) ───────────────────────────
_ENV_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"
)
load_dotenv(_ENV_PATH)

logging.basicConfig(level=logging.INFO)


# ==========================================
# MAIN RUNNER  — on-demand, no email
# ==========================================

def run_no_email() -> dict:
    """
    Fetches live intraday prices RIGHT NOW (at request time) and evaluates
    alert thresholds. No email is sent.
    """
    tz    = pytz.timezone(TIMEZONE)
    now   = datetime.datetime.now(tz)
    today = now.date()

    logging.info(f"[no_email] Request received at {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    # ── Live fetch (parallel, same logic as the hourly email job) ─────────────
    all_data = fetch_all(today)
    logging.info(f"[no_email] Fetched {len(all_data)} symbol(s).")

    # ── Evaluate thresholds ───────────────────────────────────────────────────
    alerts = evaluate_alerts(all_data, today)

    # Annotate each alert row with which threshold(s) fired
    for row in alerts:
        market_cap        = row.get("market_cap", 0)
        vol_thresh        = row.get("vol_thresh", 100000 if market_cap >= 10_000_000_000 else 20000)
        row["price_trigger"]  = abs(row["pct_change"]) >= HOURLY_PRICE_CHANGE_THRESHOLD
        row["volume_trigger"] = row["volume"] >= vol_thresh

    # Round floats for clean JSON
    def _round(row: dict) -> dict:
        return {
            **row,
            "current_price": round(row.get("current_price", 0), 2),
            "prev_close":    round(row.get("prev_close", 0), 2),
            "pct_change":    round(row.get("pct_change", 0), 4),
        }

    alerts_clean   = [_round(r) for r in sorted(alerts,   key=lambda x: abs(x["pct_change"]), reverse=True)]
    all_data_clean = [_round(r) for r in sorted(all_data, key=lambda x: abs(x["pct_change"]), reverse=True)]

    return {
        "status":              "ok",
        "timestamp":           now.isoformat(),
        "symbols_checked":     len(all_data),
        "alerts_fired":        len(alerts),
        "price_threshold_pct": HOURLY_PRICE_CHANGE_THRESHOLD,
        "alerts":              alerts_clean,
        "all_data":            all_data_clean,
    }


# ==========================================
# VERCEL SERVERLESS HANDLER
# ==========================================

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            result = run_no_email()
            body   = json.dumps(result).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
        except Exception as e:
            logging.error(f"[no_email handler] Unhandled error: {e}", exc_info=True)
            error_body = json.dumps({"status": "error", "message": str(e)}).encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(error_body)))
            self.end_headers()
            self.wfile.write(error_body)

    def log_message(self, fmt, *args):
        logging.info(f"[no_email handler] {fmt % args}")

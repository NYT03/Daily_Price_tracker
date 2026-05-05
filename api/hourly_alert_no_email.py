"""
hourly_alert_no_email.py
------------------------
Identical to hourly_alert.py in data-fetching & alert evaluation but:
  - Does NOT send any email
  - Returns a rich JSON response including full per-stock details
    suitable for rendering directly on the frontend dashboard.

Vercel route: /api/hourly_alert_no_email
"""

import yfinance as yf
from concurrent.futures import ThreadPoolExecutor
import logging
import datetime
import pytz
import os
from http.server import BaseHTTPRequestHandler
from dotenv import load_dotenv
import json
from stocks_manager import load_symbols

# ── Load env vars ──────────────────────────────────────────────────────────────
_ENV_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"
)
load_dotenv(_ENV_PATH)

logging.basicConfig(level=logging.INFO)

# ==========================================
# CONFIGURATION  (mirrors hourly_alert.py)
# ==========================================
INDEX_SYMBOLS = ["^NSEI", "NIFTY_SME_EMERGE.NS"]
TIMEZONE = "Asia/Kolkata"
HOURLY_PRICE_CHANGE_THRESHOLD = 5.0   # percent


# ==========================================
# DATA FETCHING  (identical to hourly_alert)
# ==========================================

def _get_prev_close(ticker, today_date) -> float | None:
    try:
        df = ticker.history(period="10d", interval="1d")
        if df.empty:
            return ticker.info.get("previousClose")
        df_prev = df[df.index.date < today_date]
        if df_prev.empty:
            return ticker.info.get("previousClose")
        return float(df_prev.iloc[-1]["Close"])
    except Exception:
        try:
            return ticker.info.get("previousClose")
        except Exception:
            return None


def _fetch_symbol(symbol: str, today_date) -> dict | None:
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="1d", interval="1m")
        if df.empty:
            return None

        df_today = df[df.index.date == today_date]
        if df_today.empty:
            return None

        current_price = float(df_today.iloc[-1]["Close"])
        cum_volume    = int(df_today["Volume"].sum())
        prev_close    = _get_prev_close(ticker, today_date)
        if prev_close is None or prev_close == 0:
            return None

        pct_change = ((current_price - prev_close) / prev_close) * 100
        market_cap = ticker.info.get("marketCap", 0)

        return {
            "symbol":        symbol,
            "current_price": round(current_price, 2),
            "prev_close":    round(prev_close, 2),
            "pct_change":    round(pct_change, 4),
            "volume":        cum_volume,
            "market_cap":    market_cap,
        }
    except Exception as e:
        logging.warning(f"[no_email] Error fetching {symbol}: {e}")
        return None


def fetch_all(today_date) -> list[dict]:
    company_symbols = load_symbols()
    all_symbols = company_symbols + INDEX_SYMBOLS
    results = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {
            executor.submit(_fetch_symbol, sym, today_date): sym
            for sym in all_symbols
        }
        for future in futures:
            data = future.result()
            if data is not None:
                results.append(data)
    return results


# ==========================================
# ALERT EVALUATION
# ==========================================

def evaluate_alerts(all_data: list[dict], today_date: datetime.date) -> list[dict]:
    alerts = []
    for row in all_data:
        market_cap = row.get("market_cap", 0)
        vol_thresh = 100000 if market_cap >= 10_000_000_000 else 20000
        price_ok  = abs(row["pct_change"]) >= HOURLY_PRICE_CHANGE_THRESHOLD
        volume_ok = row["volume"] >= vol_thresh
        if price_ok or volume_ok:
            row["vol_thresh"]     = vol_thresh
            row["price_trigger"]  = price_ok
            row["volume_trigger"] = volume_ok
            alerts.append(row)
    return alerts


# ==========================================
# MAIN RUNNER — returns plain dict, no email
# ==========================================

def run_no_email():
    tz       = pytz.timezone(TIMEZONE)
    now      = datetime.datetime.now(tz)
    today    = now.date()

    logging.info(f"[no_email] Triggered at {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    all_data = fetch_all(today)
    logging.info(f"[no_email] Fetched data for {len(all_data)} symbol(s).")

    alerts = evaluate_alerts(all_data, today)

    return {
        "status":                "ok",
        "timestamp":             now.isoformat(),
        "symbols_checked":       len(all_data),
        "alerts_fired":          len(alerts),
        "price_threshold_pct":   HOURLY_PRICE_CHANGE_THRESHOLD,
        "alerts":                sorted(alerts, key=lambda x: abs(x["pct_change"]), reverse=True),
        "all_data":              sorted(all_data, key=lambda x: abs(x["pct_change"]), reverse=True),
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
            error_body = json.dumps({"status": "error", "message": str(e)}).encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(error_body)))
            self.end_headers()
            self.wfile.write(error_body)

    def log_message(self, format, *args):
        logging.info(f"[no_email handler] {format % args}")

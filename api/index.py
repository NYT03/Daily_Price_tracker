import yfinance as yf
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import requests
import datetime
import pytz
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from http.server import BaseHTTPRequestHandler
from dotenv import load_dotenv

# Load .env from the project root (one level above this api/ folder)
_ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
load_dotenv(_ENV_PATH)
# ==========================================
# CONFIGURATION
# ==========================================
COMPANY_SYMBOLS = [
    "AVPINFRA-SM.NS", "SRM.NS", "SAHASRA-SM.NS", "KAYNES.NS", 
    "AIRFLOA.BO", "TITAGARH.NS", "BEML.NS", "ZODIAC.NS", "SAHAJSOLAR-SM.NS",
    "SOLARIUM.BO", "GULPOLY.BO", "GAEL.BO", "SUKHJITS.NS", 
    "SRSOLTD.BO", "PRIMECAB-SM.NS", "DYCL.BO", "VMARCIND-SM.NS"
]

INDEX_SYMBOLS = ["^NSEI", "NIFTY_SME_EMERGE.NS"]  # Optional: currently unused directly but included for completeness

ALL_SYMBOLS = COMPANY_SYMBOLS + INDEX_SYMBOLS

# PASTE YOUR APPS SCRIPT WEB APP URL HERE:
APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbyhdbIDYjOOJab7OibhytbpM0ys0aINPRjXo4o0j3_sQDCo9wh4rhTzdZdM-c9KX0db/exec"

FETCH_TIMES = ["09:30", "11:00", "12:30", "14:30", "15:00"]
TIMEZONE = "Asia/Kolkata"

# ==========================================
# EMAIL ALERT CONFIGURATION
# ==========================================
SMTP_SERVER   = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT     = int(os.environ.get("SMTP_PORT", 587))
SMTP_EMAIL    = os.environ.get("SMTP_EMAIL", "example@gmail.com")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "123456")
# Comma-separated list of recipients, e.g. "a@gmail.com,b@gmail.com"
TO_EMAILS     = [e.strip() for e in os.environ.get("TO_EMAIL", "example2@gmail.com").split(",") if e.strip()]
PRICE_CHANGE_THRESHOLD = 5.0   # percent — alert if |change| >= this value

# ==========================================
# FETCHING LOGIC
# ==========================================
def get_last_traded_price(ticker, today_date):
    """
    Fetches the closing price from the most recent trading day.
    Looks back up to 5 days to find a valid close price.
    Returns (price, was_today) tuple.
    """
    # Try fetching last 5 days of daily data to find last traded price
    df_hist = ticker.history(period="5d", interval="1d")
    if df_hist.empty:
        return None, False

    # Filter out today's row if present (we want last *previous* day's close)
    df_prev = df_hist[df_hist.index.date < today_date]
    if df_prev.empty:
        # If only today is available (rare edge case), use it
        df_prev = df_hist

    last_close = float(df_prev.iloc[-1]['Close'])
    return last_close, False


def fetch_single(symbol, target_slot, now_dt):
    try:
        ticker = yf.Ticker(symbol)
        today_date = now_dt.date()

        # ── Step 1: Try intraday 1-minute data for today ──────────────────────
        df = ticker.history(period="1d", interval="1m")

        if df.empty:
            # No intraday data at all — fall back to last known daily close
            last_price, _ = get_last_traded_price(ticker, today_date)
            if last_price is None:
                return {"symbol": symbol, "error": "No data available"}
            return {"symbol": symbol, "price": last_price, "volume": 0}

        # Filter to today's rows only
        df_today = df[df.index.date == today_date]

        # ── Step 2: Stock not traded at all today ─────────────────────────────
        if df_today.empty:
            # Use last available intraday row if it falls within the last 5 days,
            # otherwise pull from the multi-day daily history.
            last_price, _ = get_last_traded_price(ticker, today_date)
            if last_price is None:
                # Final fallback: use whatever is in the intraday df
                last_price = float(df.iloc[-1]['Close'])
            return {"symbol": symbol, "price": last_price, "volume": 0}

        # ── Step 3: Stock has data today ──────────────────────────────────────
        latest_price = float(df_today.iloc[-1]['Close'])
        cum_vol = int(df_today['Volume'].sum())

        # ── Step 4: Detect no price-change (illiquid / stale tick) ───────────
        # If volume is 0 for today, it means the market reported a stale price.
        # Keep the last known price but set volume = 0 to signal no activity.
        if cum_vol == 0:
            # Try to get a cleaner price from multi-day history
            hist_price, _ = get_last_traded_price(ticker, today_date)
            if hist_price is not None:
                latest_price = hist_price
            return {"symbol": symbol, "price": latest_price, "volume": 0}

        return {
            "symbol": symbol,
            "price": latest_price,
            "volume": cum_vol
        }
    except Exception as e:
        return {"symbol": symbol, "error": str(e)}

def fetch_all_data(target_slot, now_dt):
    results = {}
    all_symbols = ALL_SYMBOLS
    
    # ThreadPool for parallel downloading (Vercel Serverless requires speed)
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_single, sym, target_slot, now_dt): sym for sym in all_symbols}
        for future in futures:
            data = future.result()
            if "error" not in data:
                results[data["symbol"]] = data
    return results

# ==========================================
# PRICE CHANGE ALERT LOGIC
# ==========================================
def get_day_open_price(symbol, today_date):
    """
    Returns the market-open price for `symbol` on `today_date`.
    Uses the first available 1-minute candle for the day.
    Falls back to the daily 'Open' field if 1-minute data is unavailable.
    """
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="1d", interval="1m")
        if not df.empty:
            df_today = df[df.index.date == today_date]
            if not df_today.empty:
                return float(df_today.iloc[0]['Open'])
        # Fallback: daily open
        df_daily = ticker.history(period="5d", interval="1d")
        if not df_daily.empty:
            df_today_d = df_daily[df_daily.index.date == today_date]
            if not df_today_d.empty:
                return float(df_today_d.iloc[0]['Open'])
    except Exception:
        pass
    return None


# ==========================================
# ALERT DEDUPLICATION (one email per stock per day)
# ==========================================
_ALERTS_LEDGER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sent_alerts.json")

def _load_sent_alerts(date_str):
    """Returns the set of symbols already alerted on the given date."""
    try:
        with open(_ALERTS_LEDGER, "r") as f:
            ledger = json.load(f)
        return set(ledger.get(date_str, []))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()

def _save_sent_alerts(date_str, symbols):
    """Appends `symbols` to the ledger for `date_str` and prunes old dates."""
    try:
        with open(_ALERTS_LEDGER, "r") as f:
            ledger = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        ledger = {}

    existing = set(ledger.get(date_str, []))
    existing.update(symbols)
    ledger[date_str] = list(existing)

    # Keep only the last 7 days to avoid unbounded growth
    cutoff = (datetime.date.today() - datetime.timedelta(days=7)).isoformat()
    ledger = {d: v for d, v in ledger.items() if d >= cutoff}

    with open(_ALERTS_LEDGER, "w") as f:
        json.dump(ledger, f, indent=2)


def check_and_alert_price_changes(market_data, now_dt):
    """
    Compares each stock's current price against today's open price.
    Sends an alert email for any stock whose change >= PRICE_CHANGE_THRESHOLD %.
    Each stock is alerted at most ONCE per calendar day.
    """
    today_date = now_dt.date()
    date_str   = today_date.isoformat()
    already_sent = _load_sent_alerts(date_str)
    alerts = []

    def _check(symbol, current_price):
        # Skip if we already sent an alert for this symbol today
        if symbol in already_sent:
            return None
        open_price = get_day_open_price(symbol, today_date)
        if open_price and open_price > 0:
            pct_change = ((current_price - open_price) / open_price) * 100
            if abs(pct_change) >= PRICE_CHANGE_THRESHOLD:
                return {
                    "symbol": symbol,
                    "open_price": open_price,
                    "current_price": current_price,
                    "pct_change": pct_change
                }
        return None

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [
            executor.submit(_check, sym, info["price"])
            for sym, info in market_data.items()
            if "price" in info
        ]
        for f in futures:
            result = f.result()
            if result:
                alerts.append(result)

    if alerts:
        send_price_alert_email(alerts, now_dt)
        # Record these symbols so they won't trigger another email today
        _save_sent_alerts(date_str, [a["symbol"] for a in alerts])
        logging.info(f"Alerted {len(alerts)} symbol(s) today; ledger updated.")
    return alerts


def send_price_alert_email(alerts, now_dt):
    """Sends an HTML email listing all stocks with >5% intraday price change."""
    if not SMTP_EMAIL or not SMTP_PASSWORD:
        logging.warning("SMTP credentials not set — skipping alert email.")
        return

    date_str = now_dt.strftime("%Y-%m-%d")
    time_str = now_dt.strftime("%H:%M")

    rows_html = ""
    for a in sorted(alerts, key=lambda x: abs(x["pct_change"]), reverse=True):
        direction = "▲" if a["pct_change"] > 0 else "▼"
        color     = "#27ae60" if a["pct_change"] > 0 else "#e74c3c"
        rows_html += f"""
        <tr>
          <td style='text-align:left;font-weight:bold;padding:8px 12px;border-bottom:1px solid #eee'>{a['symbol']}</td>
          <td style='text-align:right;padding:8px 12px;border-bottom:1px solid #eee'>&#8377;{a['open_price']:.2f}</td>
          <td style='text-align:right;padding:8px 12px;border-bottom:1px solid #eee'>&#8377;{a['current_price']:.2f}</td>
          <td style='text-align:right;font-weight:bold;color:{color};padding:8px 12px;border-bottom:1px solid #eee'>{direction} {abs(a['pct_change']):.2f}%</td>
        </tr>"""

    html = f"""
    <html><head><meta charset='UTF-8'></head>
    <body style='margin:0;padding:0;background:#f4f6f9;font-family:Arial,sans-serif'>
      <table width='100%' cellpadding='0' cellspacing='0' style='background:#f4f6f9;padding:30px 0'>
        <tr><td align='center'>
          <table width='620' cellpadding='0' cellspacing='0'
                 style='background:#ffffff;border-radius:8px;overflow:hidden;
                        box-shadow:0 2px 8px rgba(0,0,0,0.08)'>
            <!-- Header -->
            <tr>
              <td style='background:#c0392b;padding:24px 32px'>
                <h2 style='margin:0;color:#ffffff;font-size:20px'>&#9888; Price Alert — {date_str} @ {time_str}</h2>
                <p style='margin:6px 0 0;color:#f8c8c8;font-size:13px'>
                  {len(alerts)} stock(s) moved more than {PRICE_CHANGE_THRESHOLD}% from today's open
                </p>
              </td>
            </tr>
            <!-- Table -->
            <tr>
              <td style='padding:24px 32px'>
                <table width='100%' cellpadding='0' cellspacing='0'
                       style='border-collapse:collapse;font-size:14px'>
                  <thead>
                    <tr style='background:#f8f9fa'>
                      <th style='text-align:left;padding:10px 12px;color:#555;font-weight:600;border-bottom:2px solid #dee2e6'>Symbol</th>
                      <th style='text-align:right;padding:10px 12px;color:#555;font-weight:600;border-bottom:2px solid #dee2e6'>Open</th>
                      <th style='text-align:right;padding:10px 12px;color:#555;font-weight:600;border-bottom:2px solid #dee2e6'>Current</th>
                      <th style='text-align:right;padding:10px 12px;color:#555;font-weight:600;border-bottom:2px solid #dee2e6'>Change</th>
                    </tr>
                  </thead>
                  <tbody>{rows_html}</tbody>
                </table>
              </td>
            </tr>
            <!-- Footer -->
            <tr>
              <td style='background:#f8f9fa;padding:16px 32px;text-align:center;
                         color:#999;font-size:12px;border-top:1px solid #eee'>
                Atlas Capital Automation &bull; Intraday Alert System
              </td>
            </tr>
          </table>
        </td></tr>
      </table>
    </body></html>
    """

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"🚨 Price Alert: {len(alerts)} stock(s) moved >{PRICE_CHANGE_THRESHOLD}% | {date_str}"
        msg["From"]    = SMTP_EMAIL
        msg["To"]      = ", ".join(TO_EMAILS)   # shows all recipients in the header
        msg.attach(MIMEText(html, "html"))

        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_EMAIL, SMTP_PASSWORD)
        server.sendmail(SMTP_EMAIL, TO_EMAILS, msg.as_string())  # list delivers to each
        server.quit()
        logging.info(f"Price alert email sent to {len(TO_EMAILS)} recipient(s) for {len(alerts)} stocks.")
    except Exception as e:
        logging.error(f"Failed to send price alert email: {e}")


def get_target_slot(now):
    time_str = now.strftime("%H:%M")
    
    # Check if exact match
    if time_str in FETCH_TIMES:
        return time_str
             
    # Snap manual/delayed requests to the closest mathematical FETCH_TIMES slot
    now_minutes = now.hour * 60 + now.minute
    closest_slot = None
    min_diff = float('inf')
    
    for ft in FETCH_TIMES:
        parts = ft.split(":")
        ft_minutes = int(parts[0]) * 60 + int(parts[1])
        diff = abs(now_minutes - ft_minutes)
        if diff < min_diff:
            min_diff = diff
            closest_slot = ft
            
    return closest_slot

# ==========================================
# SERVERLESS HANDLER
# ==========================================
class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        tz = pytz.timezone(TIMEZONE)
        now = datetime.datetime.now(tz)
        
        target_slot = get_target_slot(now)
        date_str = now.strftime("%Y-%m-%d")
        
        # Security/Validity Check
        if not target_slot:
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"Tracker running successfully, but current time does not match a schedule window. No fetch performed.")
            return

        # Fetch Data
        market_data = fetch_all_data(target_slot, now)
        if not market_data:
            self.send_response(500)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"Failed to fetch market data from Yahoo Finance.")
            return

        # ── Price Change Alert ────────────────────────────────────────────────
        alerts = check_and_alert_price_changes(market_data, now)

        # Payload structure expected by Google Apps Script
        payload = {
            "date": date_str,
            "time_slot": target_slot,
            "companies": ALL_SYMBOLS,
            "fetch_times": FETCH_TIMES,
            "data": market_data
        }
        print(payload)
        # Shoot data directly to Google Sheet via the Apps Script API we deployed
        try:
            resp = requests.post(APPS_SCRIPT_URL, json=payload, headers={"Content-Type": "application/json"})
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            self.send_response(500)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(f"Error communicating with Google Sheets Apps Script: {str(e)}".encode('utf-8'))
            return
            
        # Successfully finished
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        
        response_data = {
            "status": "success",
            "time_slot": target_slot,
            "symbols_fetched": len(market_data),
            "price_alerts_sent": len(alerts)
        }
        self.wfile.write(json.dumps(response_data).encode('utf-8'))
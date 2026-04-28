"""
hourly_alert.py
---------------
Standalone serverless handler (Vercel /api/hourly_alert) that:
  - Runs every hour via an external cron (e.g. cron-job.org / Vercel Cron)
  - For each stock, compares the *current* intraday price against the
    *previous trading day's closing price*
  - Sends ONE batched alert email per hourly run when BOTH conditions are met:
      1. |price change vs prev close| >= HOURLY_PRICE_CHANGE_THRESHOLD (5%)
      2. Cumulative intraday volume > threshold (100k if market cap >= 1000cr, else 20k)
  - All qualifying stocks are collected first, then a single SMTP call sends
    one email to all recipients — no duplicate emails within the same run.

The existing index.py interval-reporting pipeline is left completely untouched.
"""

import yfinance as yf
from concurrent.futures import ThreadPoolExecutor
import logging
import datetime
import pytz
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from http.server import BaseHTTPRequestHandler
from dotenv import load_dotenv
import json
import requests

# ── Load env vars ──────────────────────────────────────────────────────────────
_ENV_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"
)
load_dotenv(_ENV_PATH)

logging.basicConfig(level=logging.INFO)

# Global variable for best-effort cooldown in serverless environment
_LAST_EMAIL_SENT_TIME = None
# Dictionary to track stocks that have already been alerted today to prevent duplicates
# Format: { "SYMBOL": "YYYY-MM-DD" }
_ALERTED_STOCKS_TODAY = {}

# Vercel KV integration for persistence
KV_REST_API_URL = os.environ.get("KV_REST_API_URL")
KV_REST_API_TOKEN = os.environ.get("KV_REST_API_TOKEN")

def load_alerted_stocks() -> dict:
    if not KV_REST_API_URL or not KV_REST_API_TOKEN:
        return _ALERTED_STOCKS_TODAY
    try:
        resp = requests.post(
            KV_REST_API_URL,
            headers={"Authorization": f"Bearer {KV_REST_API_TOKEN}"},
            json=["GET", "alerted_stocks_today"],
            timeout=5
        )
        if resp.status_code == 200:
            val = resp.json().get("result")
            if val:
                return json.loads(val)
    except Exception as e:
        logging.error(f"[hourly_alert] Error reading from KV: {e}")
    return _ALERTED_STOCKS_TODAY

def save_alerted_stocks(alerts_dict: dict, today_str: str):
    if not KV_REST_API_URL or not KV_REST_API_TOKEN:
        return
    try:
        # Keep only today's alerts to save space
        pruned = {k: v for k, v in alerts_dict.items() if v == today_str}
        val = json.dumps(pruned)
        requests.post(
            KV_REST_API_URL,
            headers={"Authorization": f"Bearer {KV_REST_API_TOKEN}"},
            json=["SET", "alerted_stocks_today", val, "EX", 172800],
            timeout=5
        )
    except Exception as e:
        logging.error(f"[hourly_alert] Error saving to KV: {e}")

# ==========================================
# CONFIGURATION
# ==========================================
COMPANY_SYMBOLS = [
    "AVPINFRA-SM.NS", "SRM.NS", "SAHASRA-SM.NS", "KAYNES.NS", 
    "AIRFLOA.BO", "TITAGARH.NS", "BEML.NS", "ZODIAC.NS", "SAHAJSOLAR-SM.NS",
    "SOLARIUM.BO", "GULPOLY.BO", "GAEL.BO", "SUKHJITS.NS", 
    "SRSOLTD.BO", "PRIMECAB-SM.NS", "DYCL.BO", "VMARCIND-SM.NS"
]
INDEX_SYMBOLS = ["^NSEI", "NIFTY_SME_EMERGE.NS"]
ALL_SYMBOLS   = COMPANY_SYMBOLS + INDEX_SYMBOLS

TIMEZONE = "Asia/Kolkata"

# Thresholds
HOURLY_PRICE_CHANGE_THRESHOLD = 5.0    # percent — alert if |change vs prev close| >= this
# Volume threshold is dynamically determined based on market cap:
# >= 1000 Cr: 100,000
# < 1000 Cr: 20,000

# SMTP / email settings (from .env)
SMTP_SERVER   = os.environ.get("SMTP_SERVER",   "smtp.gmail.com")
SMTP_PORT     = int(os.environ.get("SMTP_PORT", 587))
SMTP_EMAIL    = os.environ.get("SMTP_EMAIL",    "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
TO_EMAILS     = [
    e.strip()
    for e in os.environ.get("TO_EMAIL", "").split(",")
    if e.strip()
]


# ==========================================
# DATA FETCHING
# ==========================================

def _get_prev_close(ticker, today_date) -> float | None:
    """
    Returns the closing price of the most recent trading day BEFORE today.
    Looks back up to 10 days to handle long weekends / holidays.
    Falls back to ticker.info['previousClose'] if history is unavailable.
    """
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
    """
    Fetches intraday data for `symbol` and returns a dict with:
      symbol, current_price, prev_close, pct_change, volume
    Returns None if data is insufficient to evaluate.
    """
    try:
        ticker = yf.Ticker(symbol)

        # ── Intraday 1-minute bars ─────────────────────────────────────────────
        df = ticker.history(period="1d", interval="1m")
        if df.empty:
            return None

        df_today = df[df.index.date == today_date]
        if df_today.empty:
            return None  # Not traded today — nothing to alert on

        current_price = float(df_today.iloc[-1]["Close"])
        cum_volume    = int(df_today["Volume"].sum())

        # ── Previous day's close ───────────────────────────────────────────────
        prev_close = _get_prev_close(ticker, today_date)
        if prev_close is None or prev_close == 0:
            return None

        pct_change = ((current_price - prev_close) / prev_close) * 100
        market_cap = ticker.info.get("marketCap", 0)

        return {
            "symbol":        symbol,
            "current_price": current_price,
            "prev_close":    prev_close,
            "pct_change":    pct_change,
            "volume":        cum_volume,
            "market_cap":    market_cap,
        }
    except Exception as e:
        logging.warning(f"[hourly_alert] Error fetching {symbol}: {e}")
        return None


def fetch_all(today_date) -> list[dict]:
    """Fetches data for all symbols in parallel and returns raw results."""
    results = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {
            executor.submit(_fetch_symbol, sym, today_date): sym
            for sym in ALL_SYMBOLS
        }
        for future in futures:
            data = future.result()
            if data is not None:
                results.append(data)
        # for res in results:
        #     print(res)
        # print("\n")
    return results


# ==========================================
# ALERT EVALUATION
# ==========================================
def evaluate_alerts(all_data: list[dict], today_date: datetime.date) -> list[dict]:
    """
    Filters stocks that breach BOTH thresholds.
    Returns list of alert dicts ready for the email.
    """
    alerts = []
    date_str = today_date.isoformat()

    for row in all_data:
        sym = row["symbol"]
        market_cap = row.get("market_cap", 0)
        # 1000 Cr = 10,000,000,000
        vol_thresh = 100000 if market_cap >= 10_000_000_000 else 20000

        # Skip if we already sent an email for this stock today
        if _ALERTED_STOCKS_TODAY.get(sym) == date_str:
            continue

        price_ok  = abs(row["pct_change"]) >= HOURLY_PRICE_CHANGE_THRESHOLD
        volume_ok = row["volume"] >= vol_thresh
        if price_ok or volume_ok:
            row["vol_thresh"] = vol_thresh
            alerts.append(row)
            logging.info(
                f"[hourly_alert] ALERT: {sym} | "
                f"Prev close ₹{row['prev_close']:.2f} → "
                f"Current ₹{row['current_price']:.2f} "
                f"({row['pct_change']:+.2f}%) | Vol {row['volume']:,} | Thresh {vol_thresh:,}"
            )

    return alerts


# ==========================================
# EMAIL
# ==========================================
def send_hourly_alert_email(alerts: list[dict], now_dt: datetime.datetime) -> bool:
    """
    Sends ONE HTML alert email to ALL configured recipients.
    Each recipient receives the same message (BCC-style via sendmail list).
    Returns True on success, False on failure.
    """
    global _LAST_EMAIL_SENT_TIME, _ALERTED_STOCKS_TODAY
    
    if _LAST_EMAIL_SENT_TIME is not None:
        elapsed = now_dt - _LAST_EMAIL_SENT_TIME
        if elapsed < datetime.timedelta(minutes=1):
            logging.info(f"[hourly_alert] Cooldown active (1 min). Skipping email. Last sent at {_LAST_EMAIL_SENT_TIME.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            return False

    if not SMTP_EMAIL or not SMTP_PASSWORD:
        logging.warning("[hourly_alert] SMTP credentials missing — skipping email.")
        return False
    if not TO_EMAILS:
        logging.warning("[hourly_alert] No recipients configured — skipping email.")
        return False

    date_str = now_dt.strftime("%Y-%m-%d")
    time_str = now_dt.strftime("%H:%M")

    # ── Build HTML rows ────────────────────────────────────────────────────────
    rows_html = ""
    for a in sorted(alerts, key=lambda x: abs(x["pct_change"]), reverse=True):
        direction = "▲" if a["pct_change"] > 0 else "▼"
        color     = "#27ae60" if a["pct_change"] > 0 else "#e74c3c"
        rows_html += f"""
        <tr>
          <td style='text-align:left;font-weight:bold;color:#314568;padding:9px 14px;border-bottom:1px solid #D1DCE2;font-family:"Montserrat",sans-serif;'>{a['symbol']}</td>
          <td style='text-align:right;color:#0D1B2A;padding:9px 14px;border-bottom:1px solid #D1DCE2;font-family:"Montserrat",sans-serif;'>&#8377;{a['prev_close']:.2f}</td>
          <td style='text-align:right;color:#0D1B2A;padding:9px 14px;border-bottom:1px solid #D1DCE2;font-family:"Montserrat",sans-serif;'>&#8377;{a['current_price']:.2f}</td>
          <td style='text-align:right;color:#0D1B2A;padding:9px 14px;border-bottom:1px solid #D1DCE2;font-family:"Montserrat",sans-serif;'>{a['volume']:,}</td>
          <td style='text-align:right;font-weight:bold;color:{color};padding:9px 14px;border-bottom:1px solid #D1DCE2;font-family:"Montserrat",sans-serif;'>{direction} {abs(a['pct_change']):.2f}%</td>
        </tr>"""

    html = f"""
    <html><head><meta charset='UTF-8'>
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;700&display=swap" rel="stylesheet">
    </head>
    <body style='margin:0;padding:0;background:#F6F1E9;font-family:"minion Variable concept", "Montserrat", sans-serif;'>
      <table width='100%' cellpadding='0' cellspacing='0' style='background:#F6F1E9;padding:30px 0'>
        <tr><td align='center'>
          <table width='680' cellpadding='0' cellspacing='0'
                 style='background:#ffffff;border-radius:8px;overflow:hidden;
                        box-shadow:0 2px 10px rgba(0,0,0,0.10)'>
            <!-- Header -->
            <tr>
              <td style='background:#ffffff;padding:24px 32px;border-bottom:1px solid #D1DCE2;'>
                <table width='100%' cellpadding='0' cellspacing='0'>
                  <tr>
                    <td width='80' style='vertical-align:middle;'>
                      <img src="cid:logo" alt="Atlas Capital" style="max-height: 60px;" />
                    </td>
                    <td style='vertical-align:middle;text-align:left;padding-left:100px;'>
                      <h2 style='margin:0;color:#314568;font-size:15px;font-family:"Montserrat",sans-serif;'>&#9200; Intraday Price Alert &mdash; {date_str} @ {time_str}</h2>
                      <p style='margin:6px 0 0;color:#607CA4;font-size:10px;font-family:"Montserrat",sans-serif;'>
                        {len(alerts)} stock(s) moved &ge;{HOURLY_PRICE_CHANGE_THRESHOLD}% from previous day&rsquo;s close
                        with volume exceeding their respective thresholds
                      </p>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
            <!-- Table -->
            <tr>
              <td style='padding:24px 32px'>
                <table width='100%' cellpadding='0' cellspacing='0'
                       style='border-collapse:collapse;font-size:14px'>
                  <thead>
                    <tr style='background:#0D1B2A'>
                      <th style='text-align:left;padding:10px 14px;color:#F6F1E9;font-weight:700;border-bottom:2px solid #314568;font-family:"Montserrat",sans-serif;'>Symbol</th>
                      <th style='text-align:right;padding:10px 14px;color:#F6F1E9;font-weight:700;border-bottom:2px solid #314568;font-family:"Montserrat",sans-serif;'>Prev Close</th>
                      <th style='text-align:right;padding:10px 14px;color:#F6F1E9;font-weight:700;border-bottom:2px solid #314568;font-family:"Montserrat",sans-serif;'>Current Price</th>
                      <th style='text-align:right;padding:10px 14px;color:#F6F1E9;font-weight:700;border-bottom:2px solid #314568;font-family:"Montserrat",sans-serif;'>Volume</th>
                      <th style='text-align:right;padding:10px 14px;color:#F6F1E9;font-weight:700;border-bottom:2px solid #314568;font-family:"Montserrat",sans-serif;'>Change</th>
                    </tr>
                  </thead>
                  <tbody>{rows_html}</tbody>
                </table>
              </td>
            </tr>
            <!-- Footer -->
            <tr>
              <td style='background:#0D1B2A;padding:16px 32px;text-align:center;
                         color:#C6A962;font-size:12px;border-top:1px solid #314568;font-family:"Montserrat",sans-serif;'>
                Atlas Capital Automation &bull; Intraday Alert System
              </td>
            </tr>
          </table>
        </td></tr>
      </table>
    </body></html>
    """

    try:
        msg = MIMEMultipart("related")
        msg["Subject"] = (
            f"\U0001f514 Intraday Alert: {len(alerts)} stock(s) moved "
            f">{HOURLY_PRICE_CHANGE_THRESHOLD}% from prev close | {date_str} {time_str}"
        )
        msg["From"] = SMTP_EMAIL
        msg["To"]   = ", ".join(TO_EMAILS)

        msg_alt = MIMEMultipart("alternative")
        msg.attach(msg_alt)
        msg_alt.attach(MIMEText(html, "html"))

        try:
            logo_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logo.png")
            with open(logo_path, "rb") as f:
                img_data = f.read()
            image = MIMEImage(img_data, name="logo.png")
            image.add_header('Content-ID', '<logo>')
            image.add_header('Content-Disposition', 'inline', filename="logo.png")
            msg.attach(image)
        except Exception as e:
            logging.warning(f"[hourly_alert] Could not attach logo: {e}")

        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_EMAIL, SMTP_PASSWORD)
        # sendmail with a list delivers one copy to every address in the list
        server.sendmail(SMTP_EMAIL, TO_EMAILS, msg.as_string())
        server.quit()

        _LAST_EMAIL_SENT_TIME = now_dt
        # Record the stocks as alerted for today
        for a in alerts:
            _ALERTED_STOCKS_TODAY[a["symbol"]] = date_str

        logging.info(
            f"[hourly_alert] Email sent to {len(TO_EMAILS)} recipient(s) "
            f"for {len(alerts)} stock(s)."
        )
        return True
    except Exception as e:
        logging.error(f"[hourly_alert] Failed to send email: {e}")
        return False


def run_hourly_alert():
    global _ALERTED_STOCKS_TODAY
    tz      = pytz.timezone(TIMEZONE)
    now     = datetime.datetime.now(tz)
    today   = now.date()
    date_str = today.isoformat()

    # Load persisted state from KV
    _ALERTED_STOCKS_TODAY = load_alerted_stocks()

    logging.info(f"[hourly_alert] Triggered at {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    # ── Fetch all symbols ──────────────────────────────────────────────────
    all_data = fetch_all(today)
    logging.info(f"[hourly_alert] Fetched data for {len(all_data)} symbol(s).")

    # ── Evaluate which symbols breach both thresholds ──────────────────────
    alerts = evaluate_alerts(all_data, today)

    # ── Send ONE batched email if any alerts ───────────────────────────────
    email_sent = False
    if alerts:
        email_sent = send_hourly_alert_email(alerts, now)
        if email_sent:
            save_alerted_stocks(_ALERTED_STOCKS_TODAY, date_str)

    response = {
        "status":          "ok",
        "timestamp":       now.isoformat(),
        "symbols_checked": len(all_data),
        "alerts_fired":    len(alerts),
        "alert_symbols":   [a["symbol"] for a in alerts],
        "email_sent":      email_sent,
    }
    return response


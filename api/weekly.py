from http.server import BaseHTTPRequestHandler
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import json
from dotenv import load_dotenv
import yfinance as yf
import pandas as pd

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

# Email Configuration - Set these in Vercel Environment Variables
SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SMTP_EMAIL = os.environ.get("SMTP_EMAIL", "example@gmail.com")  # e.g., your_email@gmail.com
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "123455") # App Password
# Split the TO_EMAIL by comma to allow multiple recipients
TO_EMAILS = [email.strip() for email in os.environ.get("TO_EMAIL", "example2@gmail.com").split(",") if email.strip()]

# ==========================================
# WEEKLY RETURN LOGIC  (via Yahoo Finance)
# ==========================================
def get_target_fridays():
    today = datetime.today()
    days_since_friday = (today.weekday() - 4) % 7
    current_friday = today - timedelta(days=days_since_friday)
    last_friday = current_friday - timedelta(days=7)
    return current_friday.date(), last_friday.date()

def get_closest_close(hist, target_date):
    # Convert dates to match tz-aware hist index
    if hist.index.tz is not None:
        target_dt = pd.to_datetime(target_date).tz_localize(hist.index.tz)
    else:
        target_dt = pd.to_datetime(target_date)
        
    past_dates = hist[hist.index <= target_dt]
    if past_dates.empty:
        return None, None
    closest_date = past_dates.index[-1]
    return closest_date.date(), float(past_dates.loc[closest_date, "Close"])

def calculate_single_return(symbol):
    try:
        current_friday, last_friday = get_target_fridays()
        ticker = yf.Ticker(symbol)
        
        # Using 1 month to ensure we get data for the last two Fridays
        hist = ticker.history(period="1mo")
        if hist.empty:
             return {"ticker": symbol, "error": "No data found on Yahoo Finance"}

        c_date, close_current = get_closest_close(hist, current_friday)
        l_date, close_last = get_closest_close(hist, last_friday)

        if close_current is None or close_last is None:
            return {"ticker": symbol, "error": "Missing price data for the target dates"}
            
        weekly_return = ((close_current - close_last) / close_last) * 100
        return {
            "ticker": symbol,
            "last_friday_close":   close_last,
            "last_friday_date":    str(l_date),
            "current_friday_close": close_current,
            "current_friday_date":  str(c_date),
            "weekly_return":       weekly_return
        }
    except Exception as e:
        return {"ticker": symbol, "error": str(e)}

def get_all_weekly_returns():
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(calculate_single_return, sym): sym for sym in COMPANY_SYMBOLS}
        for future in futures:
            results.append(future.result())
    return results

# ==========================================
# EMAIL LOGIC
# ==========================================
def format_html_email(results):
    current_friday, _ = get_target_fridays()
    html = f"""
    <html>
      <head>
        <style>
          table {{ border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; }}
          th, td {{ border: 1px solid #dddddd; text-align: right; padding: 8px; }}
          th {{ background-color: #f2f2f2; text-align: center; }}
          .positive {{ color: green; font-weight: bold; }}
          .negative {{ color: red; font-weight: bold; }}
          .ticker {{ text-align: left; font-weight: bold; }}
        </style>
      </head>
      <body>
        <h2>Weekly Return Report - Week Ending {current_friday}</h2>
        <table>
          <tr>
            <th>Ticker</th>
            <th>Last Friday Close</th>
            <th>Current Friday Close</th>
            <th>Weekly Return (%)</th>
          </tr>
    """
    
    for res in results:
        if "error" in res:
            html += f"<tr><td class='ticker'>{res['ticker']}</td><td colspan='3'>Error: {res['error']}</td></tr>\n"
        else:
            ret = res['weekly_return']
            color_class = "positive" if ret >= 0 else "negative"
            html += f"""
            <tr>
              <td class='ticker'>{res['ticker']}</td>
              <td>{res['last_friday_close']:.2f} <br><small>({res['last_friday_date']})</small></td>
              <td>{res['current_friday_close']:.2f} <br><small>({res['current_friday_date']})</small></td>
              <td class='{color_class}'>{ret:.2f}%</td>
            </tr>
            """
            
    html += """
        </table>
      </body>
    </html>
    """
    return html

def send_email(html_content):
    if not SMTP_EMAIL or not SMTP_PASSWORD:
        print("Email configuration missing. Skipping email send.")
        return False, "SMTP variables not set"
        
    if not TO_EMAILS:
        print("No recipient emails configured.")
        return False, "TO_EMAIL variable not set or invalid"
        
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"Weekly Stock Returns - {datetime.today().date()}"
        msg["From"] = SMTP_EMAIL
        msg["To"] = ", ".join(TO_EMAILS)

        part = MIMEText(html_content, "html")
        msg.attach(part)

        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_EMAIL, SMTP_PASSWORD)
        server.sendmail(SMTP_EMAIL, TO_EMAILS, msg.as_string())
        server.quit()
        return True, "Email sent successfully"
    except Exception as e:
        print(f"Error sending email: {e}")
        return False, str(e)

# ==========================================
# SERVERLESS HANDLER
# ==========================================
class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            print("Fetching weekly returns...")
            results = get_all_weekly_returns()
            
            print("Formatting email...")
            html_content = format_html_email(results)
            
            print("Sending email...")
            email_success, email_msg = send_email(html_content)
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            
            response_data = {
                "status": "success",
                "symbols_processed": len(results),
                "email_sent": email_success,
                "email_message": email_msg
            }
            self.wfile.write(json.dumps(response_data).encode('utf-8'))
            
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(f"An error occurred: {str(e)}".encode('utf-8'))


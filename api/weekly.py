from http.server import BaseHTTPRequestHandler
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import json

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
SMTP_EMAIL = os.environ.get("SMTP_EMAIL", "nikraval03@gmail.com")  # e.g., your_email@gmail.com
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "bdnc mgfv vprj enez") # App Password
TO_EMAIL = os.environ.get("TO_EMAIL", "nikhilraval706@gmail.com")

# ==========================================
# WEEKLY RETURN LOGIC
# ==========================================
def get_target_fridays():
    today = datetime.today()
    days_since_friday = (today.weekday() - 4) % 7
    current_friday = today - timedelta(days=days_since_friday)
    last_friday = current_friday - timedelta(days=7)
    return current_friday.date(), last_friday.date()

def get_target_close(data, target_date):
    past_data = data.loc[:pd.to_datetime(target_date)]
    if not past_data.empty:
        val = past_data['Close'].iloc[-1]
        if hasattr(val, 'item'):
            val = val.item()
        elif isinstance(val, pd.Series):
            val = val.iloc[0]
        return val, past_data.index[-1].date()
    return None, None

def calculate_single_return(ticker, start_date, end_date, current_friday, last_friday):
    try:
        data = yf.download(ticker, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'), progress=False)
        if data.empty:
            return {"ticker": ticker, "error": "No data"}
            
        if data.index.tz is not None:
            data.index = data.index.tz_localize(None)
        
        close_current, actual_current_date = get_target_close(data, current_friday)
        close_last, actual_last_date = get_target_close(data, last_friday)
        
        if close_current is not None and close_last is not None:
            weekly_return = ((close_current - close_last) / close_last) * 100
            return {
                "ticker": ticker,
                "last_friday_close": close_last,
                "last_friday_date": str(actual_last_date),
                "current_friday_close": close_current,
                "current_friday_date": str(actual_current_date),
                "weekly_return": weekly_return
            }
        else:
            return {"ticker": ticker, "error": "Insufficient data"}
    except Exception as e:
        return {"ticker": ticker, "error": str(e)}

def get_all_weekly_returns():
    current_friday, last_friday = get_target_fridays()
    start_date = last_friday - timedelta(days=10)
    end_date = current_friday + timedelta(days=1)
    
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(calculate_single_return, sym, start_date, end_date, current_friday, last_friday): sym for sym in COMPANY_SYMBOLS}
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
            html += f"<tr><td class='ticker'>{res['ticker']}</td><td colspan='3'>Error: {res['error']}</td></tr>"
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
        
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"Weekly Stock Returns - {datetime.today().date()}"
        msg["From"] = SMTP_EMAIL
        msg["To"] = TO_EMAIL

        part = MIMEText(html_content, "html")
        msg.attach(part)

        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_EMAIL, SMTP_PASSWORD)
        server.sendmail(SMTP_EMAIL, TO_EMAIL, msg.as_string())
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

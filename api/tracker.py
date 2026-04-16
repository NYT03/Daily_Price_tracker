import yfinance as yf
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import requests
import datetime
import pytz
from http.server import BaseHTTPRequestHandler

# ==========================================
# CONFIGURATION
# ==========================================
COMPANY_SYMBOLS = [
    "AVPINFRA.NS", "SRM.NS", "BRGIL.NS", "SAHASRA.NS", "KAYNES.NS", 
    "AIRFLOA.NS", "TITAGARH.NS", "BEML.NS", "ZODIAC.NS", "SAHAJSOLAR.NS",
    "SOLARIUM.NS", "GULPOLY.NS", "AMBUJACEM.NS", "SUKHJITS.NS", 
    "SILICON.NS", "MANGAL.NS", "PRIMECABLE.NS", "DYCL.NS", "VMARCIND.NS"
]

INDEX_SYMBOLS = ["^NSEI", "^CRSLSME"]  # Optional: currently unused directly but included for completeness

# PASTE YOUR APPS SCRIPT WEB APP URL HERE:
APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbyhdbIDYjOOJab7OibhytbpM0ys0aINPRjXo4o0j3_sQDCo9wh4rhTzdZdM-c9KX0db/exec"

FETCH_TIMES = ["09:15", "11:00", "12:30", "14:00", "15:45"]
TIMEZONE = "Asia/Kolkata"

# ==========================================
# FETCHING LOGIC
# ==========================================
def fetch_single(symbol):
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="1d", interval="5m")
        if df.empty: 
            return {"symbol": symbol, "error": "No data"}
            
        latest_price = float(df.iloc[-1]['Close'])
        cum_vol = int(df['Volume'].sum())
        return {
            "symbol": symbol,
            "price": latest_price,
            "volume": cum_vol
        }
    except Exception as e:
        return {"symbol": symbol, "error": str(e)}

def fetch_all_data():
    results = {}
    all_symbols = COMPANY_SYMBOLS + INDEX_SYMBOLS
    
    # ThreadPool for parallel downloading (Vercel Serverless requires speed)
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_single, sym): sym for sym in all_symbols}
        for future in futures:
            data = future.result()
            if "error" not in data:
                results[data["symbol"]] = data
    return results

def get_target_slot(now):
    time_str = now.strftime("%H:%M")
    now_dt = datetime.datetime.strptime(time_str, "%H:%M")
    
    # Find the closest time slot (within a ±15 minute window)
    for target_time in FETCH_TIMES:
         target_dt = datetime.datetime.strptime(target_time, "%H:%M")
         delta_minutes = abs((now_dt - target_dt).total_seconds() / 60.0)
         if delta_minutes <= 15:
             return target_time
             
    return None

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
        market_data = fetch_all_data()
        
        if not market_data:
            self.send_response(500)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"Failed to fetch market data from Yahoo Finance.")
            return

        # Payload structure expected by Google Apps Script
        payload = {
            "date": date_str,
            "time_slot": target_slot,
            "data": market_data
        }
        
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
            "symbols_fetched": len(market_data)
        }
        self.wfile.write(json.dumps(response_data).encode('utf-8'))

from typing import Self
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
# FETCHING LOGIC
# ==========================================
def fetch_single(symbol, target_slot, now_dt):
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="1d", interval="1m")
        if df.empty: 
            return {"symbol": symbol, "error": "No data"}
            
        # Ensure 'today' data only (not previous date)
        today_date = now_dt.date()
        df_today = df[df.index.date == today_date]
        
        if df_today.empty:
            latest_price = float(df.iloc[-1]['Close'])
            return {
                "symbol": symbol,
                "price": latest_price,
                "volume": 0
            }
            
        # Price is exactly the current real-time closing price at execution
        latest_price = float(df_today.iloc[-1]['Close'])
        
        # Cumulatively sum volume for today up to the current second
        cum_vol = int(df_today['Volume'].sum())
        
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
            "symbols_fetched": len(market_data)
        }
        self.wfile.write(json.dumps(response_data).encode('utf-8'))
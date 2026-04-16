import time
import datetime
import logging
import signal
import sys
import pytz
import os
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ==========================================
# CONFIGURATION
# ==========================================
COMPANY_SYMBOLS = ["TCS.NSE", "INFY.NSE", "HDFCBANK.NSE"]
INDEX_SYMBOLS = ["NSE_EMERGE_SYMBOL", "NIFTY_SMALLCAP_SYMBOL"]  # Provide valid Alpha Vantage symbols
ALPHA_VANTAGE_API_KEY = "YOUR_API_KEY"
GOOGLE_SHEET_NAME = "Daily Stock Tracker"  # Ensure this workbook exists or use its ID
GOOGLE_SERVICE_ACCOUNT_JSON = "service_account.json"
FETCH_TIMES = ["09:15", "11:00", "12:30", "14:00", "15:45"]  # 5 times per day
TIMEZONE = "Asia/Kolkata"

# ==========================================
# SETUP LOGGING
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("tracker.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# ==========================================
# GRACEFUL SHUTDOWN
# ==========================================
RUNNING = True

def signal_handler(sig, frame):
    global RUNNING
    logging.info("Graceful shutdown initiated. Waiting for current operations to finish...")
    RUNNING = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ==========================================
# GOOGLE SHEETS AUTHENTICATION
# ==========================================
def get_gspread_client():
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_SERVICE_ACCOUNT_JSON, scope)
    client = gspread.authorize(creds)
    return client

def ensure_sheet_exists(workbook, symbol):
    """
    Ensures a worksheet exists for the given symbol. 
    If not, it creates it with the required headers.
    """
    try:
        worksheet = workbook.worksheet(symbol)
    except gspread.exceptions.WorksheetNotFound:
        logging.info(f"Worksheet for '{symbol}' not found. Creating it.")
        worksheet = workbook.add_worksheet(title=symbol, rows="1000", cols="20")
        headers = ["Date"] + FETCH_TIMES
        worksheet.append_row(headers)
    return worksheet

# ==========================================
# DATA FETCHING LOGIC
# ==========================================
def fetch_alpha_vantage_data(symbol):
    """
    Fetches 5min intraday data for a symbol. 
    Determines the most recent active trading day in the response,
    sums the volume for that day up to the latest point, and grabs the latest price.
    """
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": "5min",
        "outputsize": "full",  # Required to see all intervals of the day to sum cumulative volume
        "apikey": ALPHA_VANTAGE_API_KEY
    }
    
    for attempt in range(3):
        if not RUNNING:
            return None
            
        try:
            response = requests.get(url, params=params, timeout=15)
            data = response.json()
            
            # Error checking
            if "Time Series (5min)" not in data:
                if "Note" in data or "Information" in data:
                    logging.warning(f"Rate limit or API note hit for {symbol}. Waiting 60s...")
                    time.sleep(60)
                    continue
                else:
                    logging.error(f"Error reading data for {symbol}: {data}")
                    return None
            
            time_series = data["Time Series (5min)"]
            sorted_times = sorted(time_series.keys())
            
            if not sorted_times:
                return None
                
            # The most recent interval
            latest_ts = sorted_times[-1]
            data_date = latest_ts[:10]  # Extracts YYYY-MM-DD
            
            cumulative_volume = 0
            latest_price = 0.0
            
            # Sum all volume for the latest date 
            for ts in sorted_times:
                if ts.startswith(data_date):
                    vol = int(time_series[ts].get("5. volume", 0))
                    price = float(time_series[ts].get("4. close", 0.0))
                    cumulative_volume += vol
                    latest_price = price
                    
            return {
                "price": latest_price,
                "volume": cumulative_volume
            }
            
        except Exception as e:
            logging.error(f"Attempt {attempt+1} failed for {symbol}: {e}")
            time.sleep(5)
            
    return None

# ==========================================
# SCHEDULER LOGIC
# ==========================================
def get_current_time_kolkata():
    tz = pytz.timezone(TIMEZONE)
    return datetime.datetime.now(tz)

completed_fetches = {}

def should_fetch(now):
    """
    Returns the target_time string if the current time matches an unfetched
    scheduled time within a 2-minute window. Otherwise returns None.
    """
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M")
    
    if date_str not in completed_fetches:
         completed_fetches[date_str] = []
         
    for target_time in FETCH_TIMES:
         # Compare minute differences
         target_dt = datetime.datetime.strptime(target_time, "%H:%M")
         now_dt = datetime.datetime.strptime(time_str, "%H:%M")
         
         delta_minutes = (now_dt - target_dt).total_seconds() / 60.0
         
         # If current time is exactly at or up to 2 mins past the target time
         if 0 <= delta_minutes <= 2 and target_time not in completed_fetches[date_str]:
             return target_time
             
    return None

def main():
    logging.info("Starting Daily Stock Tracker Background Process...")
    
    # Initialize GSpread
    try:
        gclient = get_gspread_client()
        workbook = gclient.open(GOOGLE_SHEET_NAME)
        logging.info(f"Successfully connected to Google Sheet: {GOOGLE_SHEET_NAME}")
    except Exception as e:
        logging.error(f"Failed to connect to Google Sheets. Check JSON file and share permissions. Error: {e}")
        return

    all_symbols = COMPANY_SYMBOLS + INDEX_SYMBOLS
    
    while RUNNING:
        now = get_current_time_kolkata()
        date_str = now.strftime("%Y-%m-%d")
        
        target_time = should_fetch(now)
        
        if target_time:
            logging.info(f"======= Starting fetch for time slot: {target_time} on {date_str} =======")
            
            for symbol in all_symbols:
                if not RUNNING:
                    break
                    
                logging.info(f"Fetching data for {symbol}...")
                data = fetch_alpha_vantage_data(symbol)
                
                if data:
                    vol = data["volume"]
                    price = data["price"]
                    
                    formatted_vol = f"{vol:,}"
                    formatted_price = f"{price:.2f}"
                    cell_value = f"{formatted_vol} | {formatted_price}"
                    logging.info(f"Result -> {symbol} at {target_time}: {cell_value}")
                    
                    try:
                        # Ensures sheet is present
                        worksheet = ensure_sheet_exists(workbook, symbol)
                        
                        # Find the appropriate row for today's date
                        col_date_vals = worksheet.col_values(1)
                        if date_str in col_date_vals:
                            row_idx = col_date_vals.index(date_str) + 1
                        else:
                            worksheet.append_row([date_str] + [""] * len(FETCH_TIMES))
                            row_idx = len(col_date_vals) + 1
                            
                        # Column index (+2 because it's 1-indexed and Date is column 1)
                        col_idx = FETCH_TIMES.index(target_time) + 2
                        
                        worksheet.update_cell(row_idx, col_idx, cell_value)
                        logging.info(f"Successfully updated worksheet '{symbol}'")
                    except Exception as e:
                        logging.error(f"Failed to update Google Sheet for {symbol}: {e}")
                else:
                    logging.warning(f"No usable data found for {symbol} at slot {target_time}")
                
                # Sleep between requests to respect Alpha Vantage rate limits (5 per min -> 12s sleep)
                logging.info("Sleeping 12s to respect API rate limits...")
                time.sleep(12)
                
            if RUNNING:
                completed_fetches[date_str].append(target_time)
                logging.info(f"======= Completed all updates for time slot: {target_time} =======")
                
        # Main sleep loop (checking time every 30 seconds, broken into chunks to exit quickly if Ctrl-C)
        for _ in range(6):
            if not RUNNING:
                break
            time.sleep(5)
            
    logging.info("Exited main loop safely.")

if __name__ == "__main__":
    main()
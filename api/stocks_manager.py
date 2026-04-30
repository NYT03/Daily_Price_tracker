"""
stocks_manager.py
-----------------
API endpoints for managing the COMPANY_SYMBOLS watchlist:
  GET  /api/stocks          → returns current symbols list
  POST /api/stocks          → validates & adds a symbol
  DELETE /api/stocks        → removes a symbol

Symbols are stored in stocks.json alongside this file so they persist
across serverless cold-starts (on Vercel use KV for persistence; locally
the file works fine).

Symbol validation is done via yfinance: if a ticker returns no info or
has no shortName/longName, it's considered invalid.
"""

import json
import os
import yfinance as yf
from pymongo import MongoClient

# ── Database connection ───────────────────────────────────────────────────────
_db_client = None
_db_collection = None

def _get_db():
    global _db_client, _db_collection
    if _db_client is None:
        mongo_uri = os.getenv("MONGO_URI")
        if not mongo_uri:
            print("Warning: MONGO_URI environment variable is not set.")
            return None
        try:
            _db_client = MongoClient(mongo_uri)
            # Use 'atlascapital' database and 'config' collection
            # Explicitly specify database name to avoid ambiguity
            db = _db_client["atlascapital"]
            _db_collection = db["config"]
            print(f"Successfully connected to MongoDB database: {db.name}")
        except Exception as e:
            print(f"Error connecting to MongoDB: {e}")
    return _db_collection

# ── File path for symbol storage ──────────────────────────────────────────────
_STOCKS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stocks.json")

# ── Default symbols (from main_tracker.py / hourly_alert.py) ─────────────────
_DEFAULT_SYMBOLS = [
    "AVPINFRA-SM.NS", "SRM.NS", "SAHASRA-SM.NS", "KAYNES.NS",
    "AIRFLOA.BO", "TITAGARH.NS", "BEML.NS", "ZODIAC.NS", "SAHAJSOLAR-SM.NS",
    "SOLARIUM.BO", "GULPOLY.BO", "GAEL.BO", "SUKHJITS.NS",
    "SRSOLTD.BO", "PRIMECAB-SM.NS", "DYCL.BO", "VMARCIND-SM.NS"
]


def load_symbols() -> list[str]:
    """Load symbols from MongoDB (preferred) or stocks.json; seed with defaults."""
    db_col = _get_db()
    if db_col is not None:
        try:
            doc = db_col.find_one({"key": "watchlist"})
            if doc and "symbols" in doc:
                return doc["symbols"]
            else:
                # SEED THE DATABASE: If database is connected but empty, save defaults
                print("Database is empty. Seeding with default symbols...")
                _save_symbols(list(_DEFAULT_SYMBOLS))
                return list(_DEFAULT_SYMBOLS)
        except Exception as e:
            print(f"Error loading from MongoDB: {e}")

    # Fallback to local file (e.g. for local dev or initial seed)
    if not os.path.exists(_STOCKS_FILE):
        return list(_DEFAULT_SYMBOLS)
    try:
        with open(_STOCKS_FILE, "r") as f:
            data = json.load(f)
        return data.get("symbols", list(_DEFAULT_SYMBOLS))
    except Exception:
        return list(_DEFAULT_SYMBOLS)


def _save_symbols(symbols: list[str]):
    """Persist symbols list to MongoDB and local file (if writable)."""
    db_col = _get_db()
    if db_col is not None:
        try:
            db_col.update_one(
                {"key": "watchlist"},
                {"$set": {"symbols": symbols}},
                upsert=True
            )
        except Exception as e:
            print(f"Error saving to MongoDB: {e}")

    # Attempt to save locally (will fail on Vercel, which is expected)
    try:
        with open(_STOCKS_FILE, "w") as f:
            json.dump({"symbols": symbols}, f, indent=2)
    except Exception as e:
        # On Vercel this is normal, so we just log it
        print(f"Local file write skipped (likely read-only FS): {e}")


def validate_symbol(symbol: str) -> dict:
    """
    Validate a Yahoo Finance ticker symbol.
    Returns {"valid": bool, "name": str|None, "exchange": str|None,
             "currency": str|None, "type": str|None}
    """
    symbol = symbol.strip().upper()
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        # yfinance returns a near-empty dict for invalid tickers
        name = info.get("longName") or info.get("shortName")
        quote_type = info.get("quoteType")
        exchange = info.get("exchange")
        currency = info.get("currency")
        if not name and not quote_type:
            return {"valid": False, "name": None, "exchange": None,
                    "currency": None, "type": None}
        return {
            "valid": True,
            "name": name or symbol,
            "exchange": exchange,
            "currency": currency,
            "type": quote_type,
        }
    except Exception as e:
        return {"valid": False, "name": None, "exchange": None,
                "currency": None, "type": None, "error": str(e)}


# ── Public handlers (called from index.py) ────────────────────────────────────

def handle_get_stocks() -> tuple[int, str, bytes]:
    """Return current symbol list as JSON."""
    symbols = load_symbols()
    body = json.dumps({"symbols": symbols}).encode("utf-8")
    return 200, "application/json", body


def handle_validate_symbol(symbol: str) -> tuple[int, str, bytes]:
    """Validate a single symbol and return result as JSON."""
    if not symbol:
        body = json.dumps({"error": "symbol is required"}).encode("utf-8")
        return 400, "application/json", body
    result = validate_symbol(symbol)
    body = json.dumps(result).encode("utf-8")
    return 200, "application/json", body


def handle_add_symbol(symbol: str) -> tuple[int, str, bytes]:
    """Validate and add a symbol to the list."""
    if not symbol:
        body = json.dumps({"error": "symbol is required"}).encode("utf-8")
        return 400, "application/json", body

    symbol = symbol.strip().upper()
    symbols = load_symbols()

    if symbol in symbols:
        body = json.dumps({"error": f"'{symbol}' already exists in the list"}).encode("utf-8")
        return 409, "application/json", body

    validation = validate_symbol(symbol)
    if not validation["valid"]:
        body = json.dumps({
            "error": f"'{symbol}' is not a valid Yahoo Finance symbol"
        }).encode("utf-8")
        return 422, "application/json", body

    symbols.append(symbol)
    _save_symbols(symbols)

    body = json.dumps({
        "message": f"'{symbol}' added successfully",
        "name": validation["name"],
        "symbols": symbols
    }).encode("utf-8")
    return 200, "application/json", body


def handle_remove_symbol(symbol: str) -> tuple[int, str, bytes]:
    """Remove a symbol from the list."""
    if not symbol:
        body = json.dumps({"error": "symbol is required"}).encode("utf-8")
        return 400, "application/json", body

    symbol = symbol.strip().upper()
    symbols = load_symbols()

    if symbol not in symbols:
        body = json.dumps({"error": f"'{symbol}' not found in list"}).encode("utf-8")
        return 404, "application/json", body

    symbols.remove(symbol)
    _save_symbols(symbols)

    body = json.dumps({
        "message": f"'{symbol}' removed successfully",
        "symbols": symbols
    }).encode("utf-8")
    return 200, "application/json", body

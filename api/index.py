import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse
from dotenv import load_dotenv

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Load .env from the project root (one level above this api/ folder)
_ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
load_dotenv(_ENV_PATH)

from hourly_alert import run_hourly_alert
from weekly import run_weekly_report
from main_tracker import run_main_tracker

# ==========================================
# SERVERLESS HANDLER
# ==========================================
class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_path = urlparse(self.path)
        path = parsed_path.path

        # ---------------------------------------------------------
        # ROUTE: /api/hourly_alert
        # ---------------------------------------------------------
        if path.endswith('/hourly_alert') or 'action=hourly' in parsed_path.query:
            try:
                resp_data = run_hourly_alert()
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(resp_data, indent=2).encode('utf-8'))
            except Exception as e:
                self.send_response(500)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(f"Error running hourly alert: {str(e)}".encode('utf-8'))
            return

        # ---------------------------------------------------------
        # ROUTE: /api/weekly
        # ---------------------------------------------------------
        if path.endswith('/weekly') or 'action=weekly' in parsed_path.query:
            try:
                resp_data = run_weekly_report()
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(resp_data, indent=2).encode('utf-8'))
            except Exception as e:
                self.send_response(500)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(f"Error running weekly report: {str(e)}".encode('utf-8'))
            return

        # ---------------------------------------------------------
        # ROUTE: / (Main Tracker Logic)
        # ---------------------------------------------------------
        try:
            status_code, content_type, response_bytes = run_main_tracker()
            self.send_response(status_code)
            self.send_header('Content-type', content_type)
            self.end_headers()
            self.wfile.write(response_bytes)
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(f"Error running main tracker: {str(e)}".encode('utf-8'))
        return
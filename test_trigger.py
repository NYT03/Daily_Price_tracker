import sys
import os
from io import BytesIO
import datetime
import pytz

# Add api directory to path so we can import index.py
api_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'api')
if api_path not in sys.path:
    sys.path.append(api_path)

import index

# Create a mock connection for the BaseHTTPRequestHandler
class MockConnection:
    def makefile(self, *args, **kwargs):
        return BytesIO(b"")

class MockHandler(index.handler):
    def __init__(self):
        self.client_address = ('127.0.0.1', 80)
        self.connection = MockConnection()
        self.wfile = BytesIO()
        self._headers_buffer = []

    def send_response(self, code, message=None):
        print(f"Response Status: {code}")

    def send_header(self, keyword, value):
        print(f"Header -> {keyword}: {value}")

    def end_headers(self):
        print("--- End Headers ---")

def run_test():
    print("====================================")
    print(" AUTOMATION TEST TRIGGER STARTED")
    print("====================================")
    
    # 1. Force the time so the script always triggers. 
    # By default, if the time isn't EXACTLY inside FETCH_TIMES (e.g. 09:30), 
    # your script aborts early. We mock it to "09:30" IST.
    tz = pytz.timezone("Asia/Kolkata")
    
    # The index.py handler now has snapping logic automatically!
    
    h = MockHandler()
        
    print("\n[TEST] Running handler.do_GET()... (this may take a few seconds to fetch Yahoo data)")
    h.do_GET()
        
    print("\n[TEST] Execution Finished. Server Response:")
    print("------------------------------------------------")
    print(h.wfile.getvalue().decode('utf-8'))
    print(" Please check your Google Sheet for updates!")

if __name__ == '__main__':
    run_test()

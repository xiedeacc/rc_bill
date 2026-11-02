"""Loopback-only supplier and quota fixtures. This is NOT a production quota/CRM service."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime, timedelta, timezone
import json
import threading
import uuid

lock = threading.Lock()
requests = 0
effects = set()
class Fixture(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass
    def reply(self, status, value=None):
        body = json.dumps(value).encode() if value is not None else b''
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def do_GET(self):
        with lock:
            self.reply(200, {'requests': requests, 'distinctBusinessEffects': len(effects)})
    def do_POST(self):
        global requests
        self.rfile.read(min(int(self.headers.get('Content-Length', '0')), 131072))
        if self.path == '/v1/permits':
            self.reply(200, {'permitId': str(uuid.uuid4()), 'expiresAt': (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()})
        elif self.path.endswith('/release'):
            self.reply(200, {})
        elif self.path.startswith('/notify'):
            key = self.headers.get('Idempotency-Key') or uuid.uuid4().hex
            with lock:
                requests += 1
                first = key not in effects
                effects.add(key)
            if 'commit-and-drop' in self.path and first:
                self.close_connection = True  # Persisted mock effect, deliberately no HTTP response.
            else:
                self.reply(204)
        else:
            self.reply(404)

print('Local external fixtures: http://127.0.0.1:9909; GET /stats shows effects, no payloads logged.')
ThreadingHTTPServer(('127.0.0.1', 9909), Fixture).serve_forever()

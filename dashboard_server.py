"""
PolyAgent Dashboard Server
Serves a live P&L dashboard at a public URL
"""
from http.server import HTTPServer, BaseHTTPRequestHandler
import os

HTML = open("dashboard.html").read()

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/" or self.path == "/dashboard":
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(HTML.encode())
        elif self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    print(f"Dashboard live on port {port}")
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()

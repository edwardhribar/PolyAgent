from http.server import HTTPServer, BaseHTTPRequestHandler
import os

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        with open("dashboard.html", "rb") as f:
            self.wfile.write(f.read())

    def log_message(self, format, *args):
        pass

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    print(f"Dashboard live on port {port}")
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()

"""An owned, loopback HTTP relay for exact-version MinIO failure tests."""

import contextlib
import http.client
import http.server
import json
import secrets
import threading
import urllib.parse

CONTROL = "/__nereus_m6_object_fault"
MODES = {"denied", "unavailable", "corrupt", "held"}
MAX_BYTES = 65 * 1024 * 1024
HOP_HEADERS = {"connection", "content-length", "keep-alive", "proxy-connection", "te", "trailer",
               "transfer-encoding", "upgrade"}


@contextlib.contextmanager
def fault_proxy(upstream):
    address = urllib.parse.urlsplit(upstream)
    if address.scheme != "http" or address.hostname not in {"127.0.0.1", "localhost"} or address.path:
        raise ValueError("Object fault relay requires its owned loopback HTTP fixture")
    token = secrets.token_hex(16)
    lock = threading.Lock()
    rule = None

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, _format, *_args):
            pass

        def reply(self, status, data=b"", headers=()):
            self.close_connection = True
            self.send_response_only(status)
            for name, value in headers:
                if name.lower() not in HOP_HEADERS:
                    self.send_header(name, value)
            self.send_header("Connection", "close")
            length = next((v for k, v in headers if k.lower() == "content-length"), "0")
            self.send_header("Content-Length", length if self.command == "HEAD" else str(len(data)))
            self.end_headers()
            if data and self.command != "HEAD":
                self.wfile.write(data)

        def control(self):
            nonlocal rule
            if not secrets.compare_digest(self.headers.get("X-Nereus-Test-Token", ""), token):
                return self.reply(403)
            with lock:
                if self.command == "POST":
                    size = int(self.headers.get("Content-Length", "0"))
                    if not 0 < size <= 4096:
                        return self.reply(400)
                    data = json.loads(self.rfile.read(size))
                    if (not isinstance(data, dict) or set(data) != {"path", "version", "mode"}
                            or any(not isinstance(value, str) for value in data.values())
                            or data["mode"] not in MODES or not data["path"].startswith("/") or not data["version"]):
                        return self.reply(400)
                    if rule is not None:
                        rule["release"].set()
                    rule = dict(data, hits=0, release=threading.Event())
                elif self.command == "DELETE":
                    if rule is not None:
                        rule["release"].set()
                    rule = None
                elif self.command != "GET":
                    return self.reply(405)
                state = {"hits": rule["hits"] if rule else 0}
            return self.reply(200, json.dumps(state).encode("ascii"), [("Content-Type", "application/json")])

        def relay(self):
            if self.path == CONTROL:
                return self.control()
            url = urllib.parse.urlsplit(self.path)
            version = urllib.parse.parse_qs(url.query).get("versionId")
            with lock:
                exact = (rule if rule and self.command == "GET" and version == [rule["version"]]
                         and urllib.parse.unquote(url.path) == rule["path"] else None)
                if exact is not None:
                    exact["hits"] += 1
            mode = exact["mode"] if exact else None
            if mode in {"denied", "unavailable"}:
                status, code = (403, "AccessDenied") if mode == "denied" else (503, "SlowDown")
                return self.reply(status, f"<Error><Code>{code}</Code></Error>".encode("ascii"),
                                  [("Content-Type", "application/xml")])
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 <= size <= MAX_BYTES or self.headers.get("Transfer-Encoding"):
                return self.reply(413)
            body = self.rfile.read(size) if size else None
            headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP_HEADERS}
            conn = http.client.HTTPConnection(address.hostname, address.port, timeout=30)
            try:
                conn.request(self.command, self.path, body=body, headers=headers)
                response = conn.getresponse()
                content = response.read(MAX_BYTES + 1)
                if len(content) > MAX_BYTES:
                    return self.reply(502)
                response_headers = response.getheaders()
                if mode == "held" and not exact["release"].wait(30):
                    return self.reply(504)
                if mode == "corrupt" and response.status == 200 and content:
                    content = bytes([content[0] ^ 1]) + content[1:]
                    response_headers = [(k, v) for k, v in response_headers
                                        if not k.lower().startswith("x-amz-checksum-") and k.lower() != "content-md5"]
                return self.reply(response.status, content, response_headers)
            finally:
                conn.close()

        do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = relay

    with http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, name="nereus-m6-minio-fault-relay", daemon=True)
        thread.start()
        try:
            endpoint = f"http://127.0.0.1:{server.server_port}"
            yield endpoint, endpoint + CONTROL, token
        finally:
            with lock:
                if rule is not None:
                    rule["release"].set()
            server.shutdown()
            thread.join()

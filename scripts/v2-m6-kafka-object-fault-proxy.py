"""An owned, loopback HTTP relay for isolated MinIO failure tests."""

import contextlib
import http.client
import http.server
import json
import os
import queue
import secrets
import socket
import socketserver
import subprocess
import threading
import urllib.parse

CONTROL = "/__nereus_m6_object_fault"
FIRST_LOST_PUT_SUFFIX = {"first-put-response-lost": ".nwg", "first-pulsar-data-put-response-lost": "/data"}
LOST_PUT_MODES = {"put-response-lost", *FIRST_LOST_PUT_SUFFIX}
LOST_DELETE = "delete-response-lost"
LOST_RESPONSE_MODES = LOST_PUT_MODES | {LOST_DELETE}
MODES = {"denied", "unavailable", "corrupt", "held", "held-unversioned", "observe", "bucket-unavailable"} | LOST_RESPONSE_MODES
MAX_BYTES = 65 * 1024 * 1024
HOP_HEADERS = {"connection", "content-length", "keep-alive", "proxy-connection", "te", "trailer",
               "transfer-encoding", "upgrade"}


@contextlib.contextmanager
def minio_loopback(container):
    """Copy TCP bytes to the owned container, bypassing Docker Desktop's published-port proxy."""
    # Preserve the original signed S3 bytes. No TTY and no HTTP handling on this hop.
    command = ["docker", "exec", "-i", container, "bash", "-c",
               "exec 3<>/dev/tcp/127.0.0.1/9000 || exit; exec 4<&0; "
               "cat <&3 & down=$!; cat <&4 >&3 & up=$!; "
               "wait -n \"$down\" \"$up\"; kill \"$down\" \"$up\" 2>/dev/null || :; wait 2>/dev/null"]

    class Forward(socketserver.BaseRequestHandler):
        def handle(self):
            with subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.DEVNULL, bufsize=0) as process:
                def upload():
                    try:
                        while data := self.request.recv(65536):
                            pending = memoryview(data)
                            while pending:
                                pending = pending[process.stdin.write(pending):]
                    except OSError:
                        pass  # Either peer may close this transport.
                    finally:
                        process.stdin.close()

                upstream = threading.Thread(target=upload, daemon=True)
                upstream.start()
                try:
                    while data := os.read(process.stdout.fileno(), 65536):
                        self.request.sendall(data)
                except OSError:
                    pass
                finally:
                    with contextlib.suppress(OSError):
                        self.request.shutdown(socket.SHUT_RDWR)
                    if process.poll() is None:
                        process.terminate()
                    upstream.join()

    class ForwardServer(socketserver.ThreadingTCPServer):
        daemon_threads = True

    with ForwardServer(("127.0.0.1", 0), Forward) as server:
        thread = threading.Thread(target=server.serve_forever, name="nereus-m6-minio-loopback", daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_address[1]}"
        finally:
            server.shutdown()
            thread.join()


@contextlib.contextmanager
def fault_proxy(upstream):
    address = urllib.parse.urlsplit(upstream)
    if address.scheme != "http" or address.hostname not in {"127.0.0.1", "localhost"} or address.path:
        raise ValueError("Object fault relay requires its owned loopback HTTP fixture")
    token = secrets.token_hex(16)
    lock = threading.Lock()
    rule = None
    closing = False
    idle = queue.LifoQueue(maxsize=8)

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
                    whole_bucket = data["mode"] == "bucket-unavailable"
                    held_read = data["mode"] == "held-unversioned"
                    lost_put = data["mode"] in LOST_PUT_MODES
                    if (whole_bucket and (data["version"] != "*" or not data["path"].endswith("/")
                                          or data["path"].count("/") != 2 or data["path"] == "//")
                            or lost_put and (data["version"] != "*" or data["path"].count("/") < 2
                                             or data["path"].endswith("/") != (data["mode"] in FIRST_LOST_PUT_SUFFIX))
                            or held_read and (data["version"] != "*" or data["path"].count("/") < 2
                                              or data["path"].endswith("/"))
                            or not whole_bucket and not lost_put and not held_read and data["version"] == "*"):
                        return self.reply(400)
                    if rule is not None:
                        rule["release"].set()
                    rule = dict(data, hits=0, appliedVersion=None, selectedPath=None,
                                upstreamReads=0, release=threading.Event())
                elif self.command == "PATCH":
                    if rule is None or not (
                            rule["mode"] in LOST_RESPONSE_MODES and rule["appliedVersion"] is not None
                            or rule["mode"] == "held-unversioned" and rule["upstreamReads"] > 0):
                        return self.reply(409)
                    rule["release"].set()
                elif self.command == "DELETE":
                    if rule is not None:
                        rule["release"].set()
                    rule = None
                elif self.command != "GET":
                    return self.reply(405)
                state = {"hits": rule["hits"] if rule else 0,
                         "appliedVersion": rule["appliedVersion"] if rule else None,
                         "selectedPath": rule["selectedPath"] if rule else None,
                         "upstreamReads": rule["upstreamReads"] if rule else 0}
            return self.reply(200, json.dumps(state).encode("ascii"), [("Content-Type", "application/json")])

        def relay(self):
            if self.path == CONTROL:
                return self.control()
            url = urllib.parse.urlsplit(self.path)
            version = urllib.parse.parse_qs(url.query).get("versionId")
            path = urllib.parse.unquote(url.path)
            with lock:
                fault = None
                if (rule and rule["mode"] in FIRST_LOST_PUT_SUFFIX and self.command == "PUT"
                        and rule["selectedPath"] is None and path.startswith(rule["path"])
                        and path.endswith(FIRST_LOST_PUT_SUFFIX[rule["mode"]])):
                    rule["selectedPath"] = path
                if rule and (rule["mode"] == "bucket-unavailable" and
                             (path == rule["path"][:-1] or path.startswith(rule["path"]))
                             or rule["mode"] == "put-response-lost" and self.command == "PUT" and path == rule["path"]
                             or rule["mode"] in FIRST_LOST_PUT_SUFFIX and self.command == "PUT"
                             and path == rule["selectedPath"]
                             or rule["mode"] == "held-unversioned" and self.command == "GET"
                             and path == rule["path"] and version is None
                             or version == [rule["version"]] and path == rule["path"] and
                             (rule["mode"] == LOST_DELETE and self.command == "DELETE"
                              or rule["mode"] not in LOST_RESPONSE_MODES and self.command == "GET")):
                    fault = rule
                    fault["hits"] += 1
            mode = fault["mode"] if fault else None
            if mode in {"denied", "unavailable", "bucket-unavailable"}:
                status, code = (403, "AccessDenied") if mode == "denied" else (503, "SlowDown")
                return self.reply(status, f"<Error><Code>{code}</Code></Error>".encode("ascii"),
                                  [("Content-Type", "application/xml")])
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 <= size <= MAX_BYTES or self.headers.get("Transfer-Encoding"):
                return self.reply(413)
            body = self.rfile.read(size) if size else None
            headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP_HEADERS}
            try:
                conn = idle.get_nowait()
            except queue.Empty:
                conn = http.client.HTTPConnection(address.hostname, address.port, timeout=30)
            reusable = False
            try:
                conn.request(self.command, self.path, body=body, headers=headers)
                response = conn.getresponse()
                content = response.read(MAX_BYTES + 1)
                if len(content) > MAX_BYTES:
                    return self.reply(502)
                response_headers = response.getheaders()
                reusable = conn.sock is not None and response.isclosed()
            finally:
                with lock:
                    if reusable and not closing:
                        try:
                            idle.put_nowait(conn)
                            conn = None
                        except queue.Full:
                            pass
                if conn is not None:
                    conn.close()
            if mode in LOST_PUT_MODES:
                if response.status == 200:
                    with lock:
                        fault["appliedVersion"] = response.getheader("x-amz-version-id")
                    fault["release"].wait(30)
                # Lose the original response and every conditional retry response for this exact key.
                self.close_connection = True
                return
            if mode == LOST_DELETE:
                if response.status in (200, 204):
                    with lock:
                        fault["appliedVersion"] = fault["version"]
                    fault["release"].wait(30)
                    self.close_connection = True
                    return
            if mode == "held-unversioned":
                if response.status != 206:
                    return self.reply(response.status, content, response_headers)
                with lock:
                    fault["upstreamReads"] += 1
            if mode in {"held", "held-unversioned"} and not fault["release"].wait(30):
                return self.reply(504)
            if mode == "corrupt" and response.status == 200 and content:
                content = bytes([content[0] ^ 1]) + content[1:]
                response_headers = [(k, v) for k, v in response_headers
                                    if not k.lower().startswith("x-amz-checksum-") and k.lower() != "content-md5"]
            return self.reply(response.status, content, response_headers)

        do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = relay

        def do_PATCH(self):
            if self.path == CONTROL:
                return self.control()
            return self.reply(405)

    with http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, name="nereus-m6-minio-fault-relay", daemon=True)
        thread.start()
        try:
            endpoint = f"http://127.0.0.1:{server.server_port}"
            yield endpoint, endpoint + CONTROL, token
        finally:
            with lock:
                closing = True
                if rule is not None:
                    rule["release"].set()
            server.shutdown()
            thread.join()
            while not idle.empty():
                idle.get_nowait().close()

#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0.
"""Hold positive BookKeeper v3 wire replies for one exact fixture ledger entry."""

from contextlib import ExitStack, contextmanager, suppress
import http.server
import json
import secrets
import socket
import socketserver
import struct
import threading

OPERATIONS = {"read": 1, "write": 2}
MAX_FRAME_BYTES = 8 * 1024 * 1024


def fields(data):
    offset = 0

    def varint():
        nonlocal offset
        result = 0
        for shift in range(0, 70, 7):
            if offset >= len(data):
                raise ValueError("truncated BookKeeper field")
            byte = data[offset]
            offset += 1
            result |= (byte & 127) << shift
            if byte < 128:
                return result
        raise ValueError("oversized BookKeeper varint")

    while offset < len(data):
        tag = varint()
        number, wire = tag >> 3, tag & 7
        if not number:
            raise ValueError("BookKeeper field has no tag")
        if wire == 0:
            yield number, varint()
            continue
        length = varint() if wire == 2 else {1: 8, 5: 4}.get(wire)
        if length is None or length > len(data) - offset:
            raise ValueError("invalid BookKeeper field width")
        if wire == 2:
            yield number, data[offset:offset + length]
        offset += length


def positive_reply(frame):
    try:
        message = dict(fields(memoryview(frame)))
        header = dict(fields(message[1]))
        operation = header.get(2)
        if header.get(1) != 3 or operation not in OPERATIONS.values() or message.get(2) != 0:
            return None
        payload = dict(fields(message[99 + operation]))
        if payload.get(1) == 0 and type(payload.get(2)) is int and type(payload.get(3)) is int:
            return operation, payload[2], payload[3]
    except (KeyError, TypeError, ValueError):
        pass  # Other valid BookKeeper response families are forwarded untouched.
    return None


def receive(sock, length):
    data = bytearray(length)
    view = memoryview(data)
    while view:
        read = sock.recv_into(view)
        if not read:
            raise EOFError
        view = view[read:]
    return data


@contextmanager
def fault_relays(target_ports):
    """The BookKeeper registry still advertises its original ports; only fixture publication is relayed."""
    token = secrets.token_hex(16)
    guard = threading.Lock()
    rule = None
    connections = set()

    class Control(http.server.BaseHTTPRequestHandler):
        def log_message(self, _format, *_args):
            pass

        def dispatch(self):
            nonlocal rule
            if self.path != "/__bk_io" or not secrets.compare_digest(self.headers.get("X-Nereus-Test-Token", ""), token):
                self.send_error(403)
                return
            with guard:
                if self.command == "POST":
                    try:
                        size = int(self.headers.get("Content-Length", "0"))
                        if not 0 < size <= 256:
                            raise ValueError("invalid rule length")
                        body = json.loads(self.rfile.read(size))
                        if not isinstance(body, dict) or set(body) != {"operation", "ledger", "entry"}:
                            raise ValueError("invalid rule fields")
                        operation = OPERATIONS[body["operation"]]
                        if any(type(body[key]) is not int or not 0 <= body[key] < 1 << 63
                               for key in ("ledger", "entry")):
                            raise ValueError("invalid ledger entry")
                    except (ValueError, TypeError, KeyError):
                        self.send_error(400)
                        return
                    if rule is not None:
                        rule["release"].set()
                    rule = {"target": (operation, body["ledger"], body["entry"]),
                            "hits": 0, "peers": set(), "release": threading.Event()}
                elif self.command == "PATCH":
                    if not rule or not rule["hits"]:
                        self.send_error(409)
                        return
                    rule["release"].set()
                elif self.command == "DELETE" and rule:
                    rule["release"].set()
                    rule = None
                state = {"hits": rule["hits"] if rule else 0, "peers": sorted(rule["peers"]) if rule else []}
            encoded = json.dumps(state, sort_keys=True).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        do_GET = do_POST = do_PATCH = do_DELETE = dispatch

    class Server(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True

    class Relay(socketserver.BaseRequestHandler):
        def handle(self):
            try:
                upstream = socket.create_connection(("127.0.0.1", self.server.target_port), timeout=5)
            except OSError:
                return
            with upstream:
                upstream.settimeout(None)
                downstream = self.request
                with guard:
                    connections.update((upstream, downstream))

                def requests():
                    try:
                        while data := downstream.recv(65536):
                            upstream.sendall(data)
                    except OSError:
                        pass
                    finally:
                        with suppress(OSError):
                            upstream.shutdown(socket.SHUT_WR)

                upload = threading.Thread(target=requests, daemon=True)
                upload.start()
                try:
                    while True:
                        header = receive(upstream, 4)
                        length, = struct.unpack(">I", header)
                        if not 0 < length <= MAX_FRAME_BYTES:
                            raise ValueError("invalid BookKeeper fixture frame length")
                        response = receive(upstream, length)
                        with guard:
                            active = rule
                        if active and positive_reply(response) == active["target"]:
                            with guard:
                                active["hits"] += 1
                                active["peers"].add(self.server.server_address[1])
                            active["release"].wait()
                        downstream.sendall(header)
                        downstream.sendall(response)
                except (EOFError, OSError):
                    pass
                finally:
                    for sock in (downstream, upstream):
                        with suppress(OSError):
                            sock.shutdown(socket.SHUT_RDWR)
                    upload.join()
                    with guard:
                        connections.discard(upstream)
                        connections.discard(downstream)

    with ExitStack() as stack:
        servers = []
        for advertised, destination in target_ports.items():
            server = stack.enter_context(Server(("127.0.0.1", advertised), Relay))
            server.target_port = destination
            servers.append(server)
        control = stack.enter_context(http.server.ThreadingHTTPServer(("127.0.0.1", 0), Control))
        servers.append(control)
        threads = [threading.Thread(target=server.serve_forever, daemon=True) for server in servers]
        for thread in threads:
            thread.start()
        try:
            yield f"http://127.0.0.1:{control.server_port}/__bk_io", token
        finally:
            with guard:
                if rule:
                    rule["release"].set()
                for connection in connections:
                    with suppress(OSError):
                        connection.shutdown(socket.SHUT_RDWR)
            for server in servers:
                server.shutdown()
            for thread in threads:
                thread.join()

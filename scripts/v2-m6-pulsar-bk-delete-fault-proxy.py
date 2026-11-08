# Licensed under the Apache License, Version 2.0.
"""Inject one unknown ZooKeeper delete response while retaining its exact native transaction."""

import contextlib
import re
import socket
import socketserver
import struct
import threading


def _packet(stream):
    header = stream.read(4)
    if not header:
        return None
    if len(header) != 4:
        raise OSError("truncated ZooKeeper frame length")
    size, = struct.unpack(">i", header)
    if size <= 0 or size > 16 * 1024 * 1024:
        raise OSError("ZooKeeper frame size exceeds fixture bound")
    body = stream.read(size)
    if len(body) != size:
        raise OSError("truncated ZooKeeper frame")
    return header + body


def _deletes(packet, target):
    # Jute MultiHeader(type=delete, done=false, err=-1), then DeleteRequest path and version.
    marker = struct.pack(">i?i", 2, False, -1)
    at = packet.find(marker, 12)
    while at >= 0 and at + len(marker) + 8 <= len(packet):
        first = at + len(marker)
        length, = struct.unpack_from(">i", packet, first)
        path = packet[first + 4:first + 4 + length]
        if 0 < length <= 4096 and re.fullmatch(rb"/ledgers/(?:[0-9]+/)*L[0-9]+", path):
            if int(b"".join(re.findall(rb"[0-9]+", path))) == target:
                return True
        at = packet.find(marker, at + 1)
    return False


class DeleteCut:
    def __init__(self):
        self.lock = threading.Lock()
        self.held = threading.Event()
        self.done = threading.Event()
        self.target = None
        self.forward = None
        self.response = None
        self.matches = 0

    def arm(self, ledger):
        with self.lock:
            if self.target is not None:
                raise ValueError("delete fixture already armed")
            self.target = ledger

    def capture(self, packet, send):
        with self.lock:
            if self.target is None or not _deletes(packet, self.target):
                return False
            self.matches += 1
            if self.matches != 1:
                raise OSError("original native delete was redispatched")
            self.forward = lambda: send(packet)
            return True

    def deliver(self):
        if not self.held.wait(20):
            raise RuntimeError("original native transaction is not retained")
        with self.lock:
            if self.forward is None:
                raise RuntimeError("original native transaction was already delivered")
            send, self.forward = self.forward, None
        send()
        if not self.done.wait(20) or self.response != 0:
            raise RuntimeError("retained ZooKeeper transaction did not apply")


@contextlib.contextmanager
def delete_fault_proxy():
    cut = DeleteCut()

    class Forward(socketserver.BaseRequestHandler):
        def handle(self):
            with socket.create_connection(("127.0.0.1", 2181)) as server:
                upstream, downstream = threading.Lock(), threading.Lock()
                condition = threading.Condition()
                pending = set()
                original_xid = None

                def send(packet):
                    with upstream:
                        server.sendall(packet)

                def reply(packet):
                    with downstream:
                        self.request.sendall(packet)

                def replies():
                    try:
                        with server.makefile("rb") as stream:
                            handshake = True
                            while packet := _packet(stream):
                                if handshake:
                                    reply(packet)
                                    handshake = False
                                    continue
                                xid, _, code = struct.unpack_from(">iqi", packet, 4)
                                if xid == original_xid:
                                    cut.response = code
                                    cut.done.set()
                                    continue
                                reply(packet)
                                with condition:
                                    pending.discard(xid)
                                    condition.notify_all()
                    except OSError:
                        pass
                    finally:
                        with contextlib.suppress(OSError):
                            self.request.shutdown(socket.SHUT_RDWR)

                receiver = threading.Thread(target=replies, daemon=True)
                receiver.start()
                try:
                    with self.request.makefile("rb") as stream:
                        handshake = True
                        while packet := _packet(stream):
                            if handshake:
                                send(packet)
                                handshake = False
                                continue
                            xid, op = struct.unpack_from(">ii", packet, 4)
                            if op == 14 and cut.capture(packet, send):
                                with condition:
                                    if not condition.wait_for(lambda: not pending, 10):
                                        raise OSError("preceding ZooKeeper replies did not drain")
                                    original_xid = xid
                                # CONNECTIONLOSS for this exact request only. Other commands remain live.
                                reply(struct.pack(">iiqi", 16, xid, -1, -4))
                                cut.held.set()
                            else:
                                if xid >= 0:
                                    with condition:
                                        pending.add(xid)
                                send(packet)
                except OSError:
                    pass
                finally:
                    with contextlib.suppress(OSError):
                        server.shutdown(socket.SHUT_RDWR)
                    receiver.join()

    class Proxy(socketserver.ThreadingTCPServer):
        daemon_threads = True

    with Proxy(("127.0.0.1", 0), Forward) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"127.0.0.1:{server.server_address[1]}", cut
        finally:
            server.shutdown()
            thread.join()

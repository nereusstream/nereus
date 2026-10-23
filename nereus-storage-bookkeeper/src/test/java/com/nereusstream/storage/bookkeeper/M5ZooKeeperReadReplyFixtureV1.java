/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.storage.bookkeeper;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** One real SDK connection; delays one original GET or SET_DATA reply in wire order. */
public final class M5ZooKeeperReadReplyFixtureV1 implements AutoCloseable {
    private static final int MAX_FRAME_BYTES = 1_048_576 + 4096;
    private final ServerSocket listener;
    private final ExecutorService transport = Executors.newFixedThreadPool(2);
    private final AtomicReference<String> target = new AtomicReference<>();
    private volatile int targetOperation = 4;
    private volatile boolean loseReply;
    private volatile boolean observeCreateOnly;
    private volatile boolean monitorLedgerCreates;
    private volatile int replyFailureCode = org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS.intValue();
    private final AtomicInteger heldXid = new AtomicInteger(Integer.MIN_VALUE);
    private final AtomicInteger writeDispatches = new AtomicInteger();
    private final AtomicInteger readDispatches = new AtomicInteger();
    private volatile int heldReadOrdinal = 1;
    private final java.util.Set<Integer> blockedGets = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private volatile boolean blockMetadataGets;
    private final CountDownLatch held = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final String upstream;
    private volatile Socket client;
    private volatile Socket server;
    private volatile boolean closing;

    public M5ZooKeeperReadReplyFixtureV1(String upstream) throws IOException {
        this.upstream = upstream;
        listener = new ServerSocket();
        listener.bind(new InetSocketAddress("127.0.0.1", 0));
        transport.submit(() -> {
            try {
                client = listener.accept();
                var uri = URI.create(upstream);
                server = new Socket(uri.getHost(), uri.getPort());
                transport.submit(() -> pump(server, client, false));
                pump(client, server, true);
            } catch (Throwable error) {
                if (!closing) {
                    failure.compareAndSet(null, error);
                }
            }
        });
    }

    public String metadataUri() {
        return "zk://127.0.0.1:" + listener.getLocalPort()
                + URI.create(upstream).getPath();
    }

    public boolean hasSdkConnection() {
        return client != null;
    }

    public void holdGet(String nativePath) {
        if (!target.compareAndSet(null, nativePath)) {
            throw new IllegalStateException("Native GET response is already armed");
        }
    }

    public void holdWrite(String nativePath) {
        targetOperation = 5;
        holdGet(nativePath);
    }

    public void holdLedgerCreate() {
        targetOperation = 14;
        holdGet("<actual-sdk-ledger-create>");
    }

    public void observeLedgerCreate() {
        observeCreateOnly = true;
        holdLedgerCreate();
    }

    public void holdReservationRead(String nativePath) {
        monitorLedgerCreates = true;
        heldReadOrdinal = 2; // P1 pre-admission read first, actual guarded SDK pre-create read second.
        holdGet(nativePath);
    }

    public void rejectGetReply() {
        if (targetOperation != 4 || held.getCount() != 0) {
            throw new IllegalStateException("Native reservation has not actually been read");
        }
        replyFailureCode = org.apache.zookeeper.KeeperException.Code.NOAUTH.intValue();
        loseReply = true;
        releaseReply();
    }

    public void loseCreateReplyAndBlockReads() {
        if (targetOperation != 14 || held.getCount() != 0) {
            throw new IllegalStateException("Native ledger metadata has not actually been created");
        }
        blockMetadataGets = true;
        loseReply = true;
        releaseReply();
    }

    public void allowMetadataReads() {
        blockMetadataGets = false;
    }

    /** The actual SET_DATA has already succeeded; only its original SDK reply becomes CONNECTIONLOSS. */
    public void loseWriteReply() {
        if (targetOperation != 5 || held.getCount() != 0) {
            throw new IllegalStateException("Native metadata write has not actually succeeded");
        }
        loseReply = true;
        releaseReply();
    }

    public boolean awaitHeld() throws InterruptedException {
        return held.await(10, TimeUnit.SECONDS) && failure.get() == null;
    }

    public int targetedWriteDispatches() {
        return writeDispatches.get();
    }

    public int targetedReadDispatches() {
        return readDispatches.get();
    }

    public void releaseReply() {
        release.countDown();
    }

    public void requireHealthy() {
        if (failure.get() != null) {
            throw new IllegalStateException("Actual ZooKeeper forwarding failed", failure.get());
        }
    }

    private void pump(Socket source, Socket destination, boolean requests) {
        try {
            var input = new DataInputStream(source.getInputStream());
            var output = new DataOutputStream(destination.getOutputStream());
            boolean handshake = true;
            while (!closing) {
                int length = input.readInt();
                if (length < 1 || length > MAX_FRAME_BYTES) {
                    throw new IOException("Native ZooKeeper frame exceeds the fixture bound");
                }
                byte[] frame = new byte[length];
                input.readFully(frame);
                if (!handshake && requests && length >= 12) {
                    var bytes = ByteBuffer.wrap(frame);
                    int xid = bytes.getInt();
                    int operation = bytes.getInt();
                    int pathBytes = bytes.getInt();
                    if (xid >= 0 && operation == 14 && (targetOperation == 14 || monitorLedgerCreates)) {
                        var ledgerPath = ledgerCreatePath(frame);
                        if (ledgerPath.isPresent()) {
                            if (target.get().equals("<actual-sdk-ledger-create>")) {
                                target.set(ledgerPath.orElseThrow());
                            }
                            if (monitorLedgerCreates || target.get().equals(ledgerPath.orElseThrow())) {
                                writeDispatches.incrementAndGet();
                                if (!observeCreateOnly && !monitorLedgerCreates) {
                                    heldXid.compareAndSet(Integer.MIN_VALUE, xid);
                                }
                            }
                        }
                    }
                    if (xid >= 0
                            && operation == 4
                            && pathBytes >= 0
                            && pathBytes <= bytes.remaining()
                            && blockMetadataGets
                            && new String(frame, 12, pathBytes, StandardCharsets.UTF_8).equals(target.get())) {
                        if (blockedGets.size() >= 16) {
                            throw new IOException("Native metadata read fault bound exceeded");
                        }
                        blockedGets.add(xid);
                    }
                    if (xid >= 0 && operation == targetOperation && pathBytes >= 0 && pathBytes <= bytes.remaining()) {
                        String path = new String(frame, 12, pathBytes, StandardCharsets.UTF_8);
                        if (path.equals(target.get())) {
                            if (operation == 5) {
                                writeDispatches.incrementAndGet();
                            }
                            if (operation != 4 || readDispatches.incrementAndGet() == heldReadOrdinal) {
                                heldXid.compareAndSet(Integer.MIN_VALUE, xid);
                            }
                        }
                    }
                } else if (!handshake
                        && !requests
                        && length >= 16
                        && ByteBuffer.wrap(frame).getInt() == heldXid.get()) {
                    if (ByteBuffer.wrap(frame).getInt(12) != 0) {
                        throw new IOException("Target native metadata operation did not succeed");
                    }
                    held.countDown();
                    if (!release.await(10, TimeUnit.SECONDS)) {
                        throw new IOException("Original native GET reply was not released");
                    }
                    if (loseReply) {
                        frame = java.util.Arrays.copyOf(frame, 16);
                        ByteBuffer.wrap(frame).putInt(12, replyFailureCode);
                        length = frame.length;
                    }
                } else if (!handshake
                        && !requests
                        && length >= 16
                        && blockedGets.remove(ByteBuffer.wrap(frame).getInt())
                        && blockMetadataGets) {
                    frame = java.util.Arrays.copyOf(frame, 16);
                    ByteBuffer.wrap(frame).putInt(12, org.apache.zookeeper.KeeperException.Code.NOAUTH.intValue());
                    length = frame.length;
                }
                handshake = false;
                output.writeInt(length);
                output.write(frame);
                output.flush();
            }
        } catch (java.io.EOFException normalClose) {
            // Actual SDK session close terminates the forwarded connection.
        } catch (Throwable error) {
            if (!closing) {
                failure.compareAndSet(null, error);
            }
        }
    }

    private java.util.Optional<String> ledgerCreatePath(byte[] frame) throws IOException {
        var operations = new org.apache.zookeeper.MultiOperationRecord();
        operations.deserialize(
                org.apache.jute.BinaryInputArchive.getArchive(
                        new java.io.ByteArrayInputStream(frame, 8, frame.length - 8)),
                "multi");
        for (var operation : operations) {
            if (operation.toRequestRecord() instanceof org.apache.zookeeper.proto.CreateRequest create) {
                byte[] prefix = "BookieMetadataFormatVersion\t".getBytes(StandardCharsets.US_ASCII);
                if (create.getData().length < prefix.length
                        || !java.util.Arrays.equals(java.util.Arrays.copyOf(create.getData(), prefix.length), prefix)) {
                    continue;
                }
                try {
                    var metadata = new org.apache.bookkeeper.meta.LedgerMetadataSerDe()
                            .parseConfig(create.getData(), 0, java.util.Optional.empty());
                    if (metadata.getCustomMetadata().containsKey(M5PulsarNativeCreateSpecV1.SCOPE_KEY)) {
                        return java.util.Optional.of(create.getPath());
                    }
                } catch (IOException notLedgerMetadata) {
                    // Native reservation/index creates carry another admitted codec.
                }
            }
        }
        return java.util.Optional.empty();
    }

    @Override
    public void close() throws Exception {
        closing = true;
        releaseReply();
        listener.close();
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
        transport.shutdownNow();
        if (!transport.awaitTermination(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Native response fixture transport did not terminate");
        }
    }
}

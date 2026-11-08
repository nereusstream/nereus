/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.pulsar.broker;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ExtentLocator;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Records one exact Provider mutation before dispatch. Closing the same key excludes later sends. */
final class PulsarM6ObjectWalDispatchFence {
    private static final Sha256Digest NO_PUBLICATION = Sha256Digest.copyOf(new byte[Sha256Digest.LENGTH]);
    private static final int FORMAT = 0x50574431; // PWD1
    private static final int MAX_KEY_BYTES = 1024;
    private static final int MAX_MUTATION_RACES = 4;
    private static final int HEADER_BYTES = Integer.BYTES + 3 + Long.BYTES + Sha256Digest.LENGTH;
    private static final int PENDING_FIELDS_BYTES = Short.BYTES + Long.BYTES + Sha256Digest.LENGTH;

    private final ExactMetadataTransactionStoreV1 metadata;
    private final WalRunRootRecord root;
    private final Duration timeout;
    private final String key;

    PulsarM6ObjectWalDispatchFence(
            ExactMetadataTransactionStoreV1 metadata,
            String bindingAuthorityRoot,
            WalRunRootRecord root,
            Duration timeout) {
        this.metadata = metadata;
        this.root = root;
        this.timeout = timeout;
        key = bindingAuthorityRoot + "/dispatch/"
                + WalRunControlCodec.rootSha256(root).toHex();
    }

    /** Only an exact fresh Root authority may install the original writer gate. Never opens a closed gate. */
    void initializeFresh() throws IOException {
        var deadline = deadline();
        var empty = new State(false, false, Optional.empty(), -1, NO_PUBLICATION);
        var before = read(deadline);
        if (before.isEmpty()) {
            if (update(before, empty, deadline) == MutationOutcome.APPLIED_EXACT) {
                return;
            }
            before = read(deadline);
        }
        if (!before.map(Value::state).equals(Optional.of(empty))) {
            throw new IOException("native Object WAL original dispatch admission was not confirmed");
        }
    }

    void arm(ObjectIdentity object) throws IOException {
        requireObject(object);
        var deadline = deadline();
        var before = read(deadline).orElseThrow(() -> new IOException("Object WAL dispatch gate is absent"));
        if (!before.state().admitted()
                || before.state().closed()
                || before.state().pending().isPresent()) {
            throw new IOException("Object WAL dispatch gate is closed or retains an unresolved mutation");
        }
        var armed = before.state().withPending(Optional.of(object));
        if (update(Optional.of(before), armed, deadline) == MutationOutcome.APPLIED_EXACT) {
            return;
        }
        var observed = read(deadline);
        if (!observed.map(Value::state).equals(Optional.of(armed))) {
            throw new IOException("Object WAL dispatch admission did not select the original candidate");
        }
    }

    /** An actual terminal Provider reply, or a complete exact positive reread, settles only the armed identity. */
    void complete(ObjectIdentity object) throws IOException {
        complete(object, false);
    }

    void completeVerified(ObjectIdentity object) throws IOException {
        complete(object, true);
    }

    private void complete(ObjectIdentity object, boolean independentlyVerified) throws IOException {
        var deadline = deadline();
        for (int attempt = 0; attempt < MAX_MUTATION_RACES; attempt++) {
            var before = read(deadline).orElseThrow(() -> new IOException("Object WAL dispatch gate is absent"));
            if ((independentlyVerified || before.state().closed())
                    && before.state().pending().isEmpty()) {
                return;
            }
            if (!before.state().pending().equals(Optional.of(object))) {
                throw new IOException("Object WAL terminal reply differs from the exact armed mutation");
            }
            var next = before.state().withPending(Optional.empty());
            var outcome = update(Optional.of(before), next, deadline);
            if (outcome == MutationOutcome.APPLIED_EXACT) {
                return;
            }
            var after = read(deadline).orElseThrow(() -> new IOException("Object WAL dispatch gate disappeared"));
            if (after.state().pending().isEmpty()) {
                return;
            }
            if (!after.state().pending().equals(Optional.of(object))) {
                throw new IOException("Object WAL terminal reply encountered another mutation");
            }
        }
        throw new IOException("Object WAL terminal mutation did not converge");
    }

    /** Admits writes only after the original empty physical checkpoint Head is durable. */
    void admitAfterPhysicalHead() throws IOException {
        long deadline = deadline();
        var before = read(deadline).orElseThrow(() -> new IOException("Pulsar Object WAL dispatch gate is absent"));
        var state = before.state();
        if (state.closed() || state.admitted() || state.pending().isPresent()) {
            throw new IOException("Pulsar Object WAL original dispatch gate cannot be admitted");
        }
        var admitted = new State(true, false, Optional.empty(), -1, NO_PUBLICATION);
        update(Optional.of(before), admitted, deadline);
        if (!read(deadline).map(Value::state).equals(Optional.of(admitted))) {
            throw new IOException("Pulsar Object WAL original dispatch gate admission remains unresolved");
        }
    }

    /** The same durable CAS that closes admission excludes a late logical publication. */
    void publish(ExtentLocator locator) throws IOException {
        requireObject(locator.identity().objectIdentity());
        long sequence = locator.identity().laneSequence();
        var digest = locatorDigest(locator);
        long deadline = deadline();
        var before = read(deadline).orElseThrow(() -> new IOException("Pulsar Object WAL dispatch gate is absent"));
        var state = before.state();
        if (!state.closed()
                && state.admitted()
                && state.pending().isEmpty()
                && sequence == state.publishedThrough()
                && digest.equals(state.lastPublished())) {
            return;
        }
        if (state.closed()
                || !state.admitted()
                || state.pending().isPresent()
                || sequence != Math.incrementExact(state.publishedThrough())) {
            throw new IOException("Pulsar Object WAL publication differs from its original dispatch frontier");
        }
        var published = new State(true, false, Optional.empty(), sequence, digest);
        update(Optional.of(before), published, deadline);
        if (!read(deadline).map(Value::state).equals(Optional.of(published))) {
            throw new IOException("Original Pulsar Object WAL publication remains unresolved");
        }
    }

    static Sha256Digest locatorDigest(ExtentLocator locator) {
        var identity = locator.identity();
        var object = identity.objectIdentity();
        String text = String.join(
                "/",
                "PULSAR_OBJECT_WAL_LOCATOR_V1",
                identity.walRunRootSha256().toHex(),
                Integer.toString(identity.laneId().code()),
                Long.toString(identity.laneSequence()),
                Long.toString(locator.position().virtualLedgerId()),
                Long.toString(locator.position().entryId()),
                Integer.toString(locator.frameOrdinal()),
                Long.toString(locator.extentOffset()),
                Long.toString(locator.extentLength()),
                locator.frameSha256(),
                Long.toString(object.bodyLength()),
                object.bodySha256().toHex(),
                object.key());
        return Sha256Digest.copyOf(com.nereusstream.storage.object.nwg1.Nwg1CommitmentsV1.sha256(
                text.getBytes(StandardCharsets.US_ASCII)));
    }

    /** Idempotently forbids dispatch without discarding a candidate which the original process may still send. */
    private State closeAdmission() throws IOException {
        var deadline = deadline();
        for (int attempt = 0; attempt < MAX_MUTATION_RACES; attempt++) {
            var before = read(deadline);
            var state = before.orElseThrow(
                            () -> new IOException("Published Pulsar Object WAL Root lacks its original dispatch gate"))
                    .state();
            if (state.closed()) {
                return state;
            }
            var next =
                    new State(state.admitted(), true, state.pending(), state.publishedThrough(), state.lastPublished());
            if (update(before, next, deadline) == MutationOutcome.APPLIED_EXACT) {
                return next;
            }
            var observed = read(deadline);
            if (observed.filter(value -> value.state().closed()).isPresent()) {
                return observed.orElseThrow().state();
            }
        }
        throw new IOException("Object WAL dispatch closure did not converge");
    }

    /**
     * Proves only a positive exact immutable body. An absent/unavailable body does not establish that a previously
     * armed request cannot arrive later. The caller cannot start physical LIST recovery in that case.
     */
    Closure closeAndRequireDrained(C1ObjectProviderSession readOnlyProvider) throws IOException {
        var state = closeAdmission();
        if (state.pending().isPresent()) {
            var exact = state.pending().orElseThrow();
            readOnlyProvider.consumeVerifiedObject(
                    exact, Optional.empty(), stream -> stream.transferTo(java.io.OutputStream.nullOutputStream()));
            completeVerified(exact);
        }
        return new Closure(state.admitted(), state.publishedThrough(), state.lastPublished());
    }

    record Closure(boolean admitted, long publishedThrough, Sha256Digest lastPublished) {}

    private record State(
            boolean admitted,
            boolean closed,
            Optional<ObjectIdentity> pending,
            long publishedThrough,
            Sha256Digest lastPublished) {
        private State {
            if (publishedThrough < -1
                    || (publishedThrough < 0) != lastPublished.isZero()
                    || !admitted && (pending.isPresent() || publishedThrough >= 0)) {
                throw new IllegalArgumentException("Invalid Pulsar Object WAL dispatch state");
            }
        }

        State withPending(Optional<ObjectIdentity> value) {
            return new State(admitted, closed, value, publishedThrough, lastPublished);
        }
    }

    private record Value(VersionedValue version, State state) {}

    private Optional<Value> read(long deadline) throws IOException {
        return await(metadata.read(key), deadline).map(value -> new Value(value, decode(value.canonicalStoredBytes())));
    }

    private MutationOutcome update(Optional<Value> before, State next, long deadline) throws IOException {
        return await(metadata.compareAndSet(before.map(Value::version), key, encode(next)), deadline);
    }

    private CanonicalBytes encode(State state) {
        var object = state.pending();
        byte[] path = object.map(value -> value.key().getBytes(StandardCharsets.US_ASCII))
                .orElseGet(() -> new byte[0]);
        var value = ByteBuffer.allocate(HEADER_BYTES + (object.isPresent() ? PENDING_FIELDS_BYTES + path.length : 0))
                .putInt(FORMAT)
                .put((byte) (state.admitted() ? 1 : 0))
                .put((byte) (state.closed() ? 1 : 0))
                .put((byte) (object.isPresent() ? 1 : 0))
                .putLong(state.publishedThrough())
                .put(state.lastPublished().bytes().toByteArray());
        object.ifPresent(identity -> value.putShort((short) path.length)
                .put(path)
                .putLong(identity.bodyLength())
                .put(identity.bodySha256().bytes().toByteArray()));
        return CanonicalBytes.copyOf(value.array());
    }

    private State decode(CanonicalBytes bytes) {
        if (bytes.length() < HEADER_BYTES || bytes.length() > HEADER_BYTES + PENDING_FIELDS_BYTES + MAX_KEY_BYTES) {
            throw new IllegalStateException("native Object WAL dispatch value exceeds its wire bounds");
        }
        var value = ByteBuffer.wrap(bytes.toByteArray());
        if (value.getInt() != FORMAT) {
            throw new IllegalStateException("unknown native Object WAL dispatch format");
        }
        boolean admitted = flag(value);
        boolean closed = flag(value);
        boolean pending = flag(value);
        long through = value.getLong();
        byte[] last = new byte[Sha256Digest.LENGTH];
        value.get(last);
        var lastDigest = Sha256Digest.copyOf(last);
        if (!pending && !value.hasRemaining()) {
            return new State(admitted, closed, Optional.empty(), through, lastDigest);
        }
        if (!pending || value.remaining() <= PENDING_FIELDS_BYTES) {
            throw new IllegalStateException("invalid native Object WAL dispatch value");
        }
        int length = Short.toUnsignedInt(value.getShort());
        if (length <= 0 || value.remaining() != length + Long.BYTES + Sha256Digest.LENGTH) {
            throw new IllegalStateException("invalid native Object WAL dispatch candidate key length");
        }
        byte[] path = new byte[length];
        value.get(path);
        long bodyLength = value.getLong();
        byte[] sha = new byte[Sha256Digest.LENGTH];
        value.get(sha);
        var object =
                new ObjectIdentity(new String(path, StandardCharsets.US_ASCII), bodyLength, Sha256Digest.copyOf(sha));
        requireObject(object);
        return new State(admitted, closed, Optional.of(object), through, lastDigest);
    }

    private static boolean flag(ByteBuffer value) {
        return switch (value.get()) {
            case 0 -> false;
            case 1 -> true;
            default -> throw new IllegalStateException("unknown native Object WAL dispatch flag");
        };
    }

    private void requireObject(ObjectIdentity object) {
        String path = object.key();
        if (!path.startsWith(root.providerConfiguration().exclusiveNamespacePrefix() + "/")
                || path.length() > MAX_KEY_BYTES
                || !StandardCharsets.US_ASCII.newEncoder().canEncode(path)
                || object.bodyLength() > root.providerConfiguration().maxObjectBodyBytes()) {
            throw new IllegalArgumentException("Object WAL dispatch candidate exceeds the exact Root");
        }
    }

    private long deadline() {
        return System.nanoTime() + timeout.toNanos();
    }

    private static <T> T await(CompletionStage<T> value, long deadline) throws IOException {
        try {
            return value.toCompletableFuture().get(Math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("native Object WAL dispatch metadata wait was interrupted", interrupted);
        } catch (ExecutionException | TimeoutException unresolved) {
            throw new IOException("native Object WAL dispatch metadata outcome is unresolved", unresolved);
        }
    }
}

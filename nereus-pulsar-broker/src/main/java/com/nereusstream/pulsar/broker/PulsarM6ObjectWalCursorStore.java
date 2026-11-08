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

package com.nereusstream.pulsar.broker;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.UnaryOperator;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;

/** One exact metadata CAS fences cursor and ledger properties across native Object WAL ownership changes. */
final class PulsarM6ObjectWalCursorStore {
    private static final int MAGIC = 0x4e504c43; // NPLC
    private static final int MAX_CURSORS = 1024;
    private static final int MAX_PROPERTIES = 256;
    private static final int MAX_SPARSE_POSITIONS = 8192;
    private static final int MAX_ACK_SET_WORDS = 128;
    private static final int MAX_TEXT = 4096;
    private final ExactMetadataTransactionStoreV1 metadata;
    private final String key;
    private final Sha256Digest incarnation;
    private final PulsarM6ObjectWalOwner owner;
    private volatile Stored current;
    private CompletableFuture<Void> settled = CompletableFuture.completedFuture(null);
    private boolean stopping;

    record CursorValue(
            Position mark,
            Set<Position> individuallyDeleted,
            Map<Position, List<Long>> batchAcks,
            Map<String, Long> properties,
            Map<String, String> cursorProperties) {
        CursorValue {
            mark = exactPosition(mark);
            individuallyDeleted = Set.copyOf(individuallyDeleted);
            batchAcks = Map.copyOf(batchAcks);
            properties = Map.copyOf(properties);
            cursorProperties = Map.copyOf(cursorProperties);
        }
    }

    record Value(long ownerEpoch, Map<String, String> ledgerProperties, Map<String, CursorValue> cursors) {
        Value {
            if (ownerEpoch <= 0) {
                throw new IllegalArgumentException("Object WAL cursor owner must be positive");
            }
            ledgerProperties = Map.copyOf(ledgerProperties);
            cursors = Map.copyOf(cursors);
        }
    }

    private record Stored(VersionedValue exact, Value value) {}

    private PulsarM6ObjectWalCursorStore(
            ExactMetadataTransactionStoreV1 metadata,
            String key,
            Sha256Digest incarnation,
            PulsarM6ObjectWalOwner owner,
            Stored current) {
        this.metadata = metadata;
        this.key = key;
        this.incarnation = incarnation;
        this.owner = owner;
        this.current = current;
    }

    static CompletionStage<Void> provision(
            ExactMetadataTransactionStoreV1 metadata, String root, Sha256Digest incarnation) {
        String key = key(root, incarnation);
        var empty = encode(incarnation, new Value(1, Map.of(), Map.of()));
        return metadata.compareAndSet(Optional.empty(), key, empty)
                .thenCompose(unused -> metadata.read(key))
                .thenAccept(found -> {
                    if (!found.map(VersionedValue::canonicalStoredBytes).equals(Optional.of(empty))) {
                        throw new IllegalStateException("Original empty Pulsar Object WAL cursor state differs");
                    }
                });
    }

    static CompletionStage<PulsarM6ObjectWalCursorStore> open(
            ExactMetadataTransactionStoreV1 metadata,
            String root,
            Sha256Digest incarnation,
            PulsarM6ObjectWalOwner owner) {
        String key = key(root, incarnation);
        return owner.requireCurrent().thenCompose(unused -> metadata.read(key)).thenCompose(found -> {
            var previous =
                    found.orElseThrow(() -> new IllegalStateException("Object WAL cursor state was not provisioned"));
            var value = decode(incarnation, previous.canonicalStoredBytes());
            if (value.ownerEpoch() > owner.epoch()) {
                throw new IllegalStateException("Native cursor open was fenced by a newer owner");
            }
            var store =
                    new PulsarM6ObjectWalCursorStore(metadata, key, incarnation, owner, new Stored(previous, value));
            if (owner.epoch() == value.ownerEpoch()) {
                return owner.requireCurrent().thenApply(unused -> store);
            }
            var takeover = new Value(owner.epoch(), value.ledgerProperties(), value.cursors());
            return store.commit(store.current, takeover)
                    .thenCompose(unused -> owner.requireCurrent())
                    .thenApply(unused -> store);
        });
    }

    Value snapshot() {
        return current.value();
    }

    synchronized CompletionStage<Void> update(UnaryOperator<Value> operation) {
        if (stopping) {
            return CompletableFuture.failedFuture(new IllegalStateException("Native Object WAL cursors are closing"));
        }
        final org.apache.pulsar.broker.storage.nereus.v2.NereusPulsarActiveFence.ValidWord nativeWord;
        try {
            nativeWord = owner.capture();
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
        var original = settled.thenCompose(unused -> {
                    if (!owner.isCurrent(nativeWord)) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("Native Object WAL cursor was fenced"));
                    }
                    var prior = current;
                    var candidate = operation.apply(prior.value());
                    if (candidate.ownerEpoch() != owner.epoch()) {
                        throw new IllegalStateException("Native cursor mutation cannot change ownership");
                    }
                    if (prior.value().equals(candidate)) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return commit(prior, candidate);
                })
                .thenRun(() -> {
                    if (!owner.isCurrent(nativeWord)) {
                        throw new IllegalStateException("Native Object WAL cursor finished under stale ownership");
                    }
                });
        settled = original.handle((unused, failure) -> null);
        return original.copy();
    }

    synchronized CompletableFuture<Void> stopAndDrain() {
        stopping = true;
        return settled.copy();
    }

    private CompletionStage<Void> commit(Stored predecessor, Value candidate) {
        var canonical = encode(incarnation, candidate);
        return metadata.compareAndSet(Optional.of(predecessor.exact()), key, canonical)
                .thenCompose(unused -> metadata.read(key))
                .thenAccept(found -> {
                    var confirmed =
                            found.orElseThrow(() -> new IllegalStateException("Native cursor head disappeared"));
                    if (!confirmed.canonicalStoredBytes().equals(canonical)) {
                        throw new IllegalStateException("Original native cursor mutation remains unresolved");
                    }
                    current = new Stored(confirmed, candidate);
                });
    }

    private static String key(String root, Sha256Digest incarnation) {
        return root + "/native-object-cursors/" + incarnation.toHex();
    }

    private static CanonicalBytes encode(Sha256Digest incarnation, Value value) {
        try (var bytes = new ByteArrayOutputStream();
                var out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.write(incarnation.bytes().toByteArray());
            out.writeLong(value.ownerEpoch());
            writeStrings(out, value.ledgerProperties());
            writeCount(out, value.cursors().size(), MAX_CURSORS);
            for (var entry : new TreeMap<>(value.cursors()).entrySet()) {
                writeText(out, entry.getKey());
                var cursor = entry.getValue();
                writePosition(out, cursor.mark());
                writeCount(out, cursor.individuallyDeleted().size(), MAX_SPARSE_POSITIONS);
                for (var position : new TreeSet<>(cursor.individuallyDeleted())) {
                    writePosition(out, position);
                }
                writeCount(out, cursor.batchAcks().size(), MAX_SPARSE_POSITIONS);
                for (var ack : new TreeMap<>(cursor.batchAcks()).entrySet()) {
                    writePosition(out, ack.getKey());
                    writeCount(out, ack.getValue().size(), MAX_ACK_SET_WORDS);
                    for (long word : ack.getValue()) {
                        out.writeLong(word);
                    }
                    requireBytes(bytes.size());
                }
                writeCount(out, cursor.properties().size(), MAX_PROPERTIES);
                for (var property : new TreeMap<>(cursor.properties()).entrySet()) {
                    writeText(out, property.getKey());
                    out.writeLong(property.getValue());
                }
                writeStrings(out, cursor.cursorProperties());
                requireBytes(bytes.size());
            }
            requireBytes(bytes.size());
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("Cannot encode native Pulsar cursor state", impossible);
        }
    }

    private static Value decode(Sha256Digest incarnation, CanonicalBytes raw) {
        requireBytes(raw.length());
        try (var in = new DataInputStream(new ByteArrayInputStream(raw.toByteArray()))) {
            if (in.readInt() != MAGIC || !incarnation.equals(Sha256Digest.copyOf(in.readNBytes(Sha256Digest.LENGTH)))) {
                throw new IllegalArgumentException("Native Pulsar cursor state type or Binding differs");
            }
            long owner = in.readLong();
            var ledger = readStrings(in);
            Map<String, CursorValue> cursors = new TreeMap<>();
            int count = readCount(in, MAX_CURSORS);
            for (int cursor = 0; cursor < count; cursor++) {
                String name = readText(in);
                var mark = readPosition(in);
                Set<Position> deleted = new TreeSet<>();
                int deletes = readCount(in, MAX_SPARSE_POSITIONS);
                for (int index = 0; index < deletes; index++) {
                    deleted.add(readPosition(in));
                }
                Map<Position, List<Long>> acks = new TreeMap<>();
                int partials = readCount(in, MAX_SPARSE_POSITIONS);
                for (int index = 0; index < partials; index++) {
                    var position = readPosition(in);
                    int words = readCount(in, MAX_ACK_SET_WORDS);
                    List<Long> ackSet = new ArrayList<>(words);
                    for (int word = 0; word < words; word++) {
                        ackSet.add(in.readLong());
                    }
                    acks.put(position, List.copyOf(ackSet));
                }
                Map<String, Long> properties = new TreeMap<>();
                int props = readCount(in, MAX_PROPERTIES);
                for (int index = 0; index < props; index++) {
                    properties.put(readText(in), in.readLong());
                }
                cursors.put(name, new CursorValue(mark, deleted, acks, properties, readStrings(in)));
            }
            var value = new Value(owner, ledger, cursors);
            if (in.read() != -1 || !raw.equals(encode(incarnation, value))) {
                throw new IllegalArgumentException("Native Pulsar cursor state is not canonical");
            }
            return value;
        } catch (IOException malformed) {
            throw new IllegalArgumentException("Invalid native Pulsar cursor state", malformed);
        }
    }

    private static Position exactPosition(Position position) {
        return PositionFactory.create(position.getLedgerId(), position.getEntryId());
    }

    private static void writePosition(DataOutputStream out, Position position) throws IOException {
        out.writeLong(position.getLedgerId());
        out.writeLong(position.getEntryId());
    }

    private static Position readPosition(DataInputStream in) throws IOException {
        return PositionFactory.create(in.readLong(), in.readLong());
    }

    private static void writeStrings(DataOutputStream out, Map<String, String> values) throws IOException {
        writeCount(out, values.size(), MAX_PROPERTIES);
        for (var property : new TreeMap<>(values).entrySet()) {
            writeText(out, property.getKey());
            writeText(out, property.getValue());
        }
    }

    private static Map<String, String> readStrings(DataInputStream in) throws IOException {
        Map<String, String> values = new TreeMap<>();
        int count = readCount(in, MAX_PROPERTIES);
        for (int index = 0; index < count; index++) {
            values.put(readText(in), readText(in));
        }
        return values;
    }

    private static void writeText(DataOutputStream out, String text) throws IOException {
        if (text.length() > MAX_TEXT) {
            throw new IllegalArgumentException("Native Pulsar cursor property exceeds its length limit");
        }
        out.writeUTF(text);
    }

    private static String readText(DataInputStream in) throws IOException {
        String value = in.readUTF();
        if (value.length() > MAX_TEXT) {
            throw new IllegalArgumentException("Native Pulsar cursor property exceeds its length limit");
        }
        return value;
    }

    private static void writeCount(DataOutputStream out, int count, int maximum) throws IOException {
        if (count < 0 || count > maximum) {
            throw new IllegalArgumentException("Native Pulsar cursor count exceeds its limit");
        }
        out.writeInt(count);
    }

    private static int readCount(DataInputStream in, int maximum) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > maximum) {
            throw new IllegalArgumentException("Native Pulsar cursor count exceeds its limit");
        }
        return count;
    }

    private static void requireBytes(int bytes) {
        if (bytes > ExactMetadataTransactionStoreV1.MAX_VALUE_BYTES) {
            throw new IllegalArgumentException("Native Pulsar cursor state exceeds its exact metadata limit");
        }
    }
}

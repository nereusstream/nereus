/*
 * Copyright 2026 NereusStream.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.kafka.bookkeeper.broker;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1.CompletedTransactionV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectCoherentProtocolSnapshotV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectExtentLocatorV1;
import com.nereusstream.kafka.bookkeeper.object.read.KafkaObjectActiveTailStateV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionProtocolStateV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaObjectMaterializedPrefixV1;
import com.nereusstream.storage.object.nwg1.Nwg1ObjectReaderV1;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Bounded complete Kafka RecordBatch reads from one captured authorized Object tail. */
public final class KafkaObjectNativeFetchV1 {
    @FunctionalInterface
    public interface Sources {
        PinnedExtent pin(KafkaObjectExtentLocatorV1 locator) throws IOException;
    }

    public interface PinnedExtent extends AutoCloseable {
        void read(Nwg1ObjectReaderV1.VerifiedFrameConsumer consumer) throws IOException;

        @Override
        void close() throws IOException;
    }

    public record Batch(long startOffset, long endOffsetExclusive, CanonicalBytes rawAssignedBytes) {}

    /** One disposable position in an exact coherent view. The cursor owns no source or buffers. */
    public record Cursor(KafkaPartitionFenceV1 fence, long version, long offset, int locator) {
        boolean matches(KafkaPartitionProtocolStateV1 root, long requested) {
            return version == root.stateVersion() && offset == requested && fence.equals(root.fence());
        }
    }

    public record Result(List<Batch> batches, List<CompletedTransactionV1> abortedTransactions, int bytes) {
        public Result {
            batches = List.copyOf(batches);
            abortedTransactions = List.copyOf(abortedTransactions);
        }
    }

    public record PositionedResult(Result result, Optional<Cursor> nextCursor) {}

    private KafkaObjectNativeFetchV1() {}

    public static Result read(
            KafkaObjectCoherentProtocolSnapshotV1 captured,
            Sources sources,
            long offset,
            int maximumBytes,
            int hardMaximumBytes,
            boolean minOneMessage,
            KafkaReadIsolationV1 isolation)
            throws IOException {
        return readPositioned(captured, sources, offset, maximumBytes, hardMaximumBytes, minOneMessage, isolation, null)
                .result();
    }

    public static PositionedResult readPositioned(
            KafkaObjectCoherentProtocolSnapshotV1 captured,
            Sources sources,
            long offset,
            int maximumBytes,
            int hardMaximumBytes,
            boolean minOneMessage,
            KafkaReadIsolationV1 isolation,
            Cursor cursor)
            throws IOException {
        Objects.requireNonNull(captured, "captured");
        Objects.requireNonNull(sources, "sources");
        Objects.requireNonNull(isolation, "isolation");
        if (maximumBytes < 0 || hardMaximumBytes <= 0) {
            throw new IllegalArgumentException("invalid native Object WAL Fetch bytes");
        }
        requireReadableOffset(captured, offset);
        long end = captured.root().readUpperBound(isolation);
        if (end <= offset || maximumBytes == 0) {
            return new PositionedResult(new Result(List.of(), List.of(), 0), Optional.empty());
        }
        var collected =
                new Collect(offset, end, Math.min(maximumBytes, hardMaximumBytes), hardMaximumBytes, minOneMessage);
        var tail = captured.activeTail();
        var locators = tail.locators();
        int index = startIndex(captured.root(), tail, offset, end, cursor);
        if (index < 0) {
            throw new IOException("native Object WAL captured prefix lacks a readable extent");
        }
        while (index < locators.size() && collected.next < end && !collected.stopped) {
            var locator = locators.get(index);
            if (locator.startOffset() > collected.next) {
                throw new IOException("native Object WAL captured locator plan has a gap");
            }
            try (var extent = Objects.requireNonNull(sources.pin(locator), "pinned extent")) {
                extent.read(collected::accept);
            }
            if (collected.next >= locator.endOffsetExclusive()) {
                index++;
            } else if (collected.next < end && !collected.stopped) {
                throw new IOException("native Object WAL extent did not cover the captured offset");
            }
        }
        if (collected.next < end && !collected.stopped) {
            throw new IOException("native Object WAL captured prefix lacks a readable extent");
        }
        long returnedEnd = collected.next;
        var aborted = abortedTransactions(captured, isolation, collected.batches, offset, returnedEnd);
        var root = captured.root();
        var next = returnedEnd > offset && returnedEnd < end
                ? Optional.of(new Cursor(root.fence(), root.stateVersion(), returnedEnd, index))
                : Optional.<Cursor>empty();
        return new PositionedResult(new Result(collected.batches, aborted, collected.bytes), next);
    }

    /** Reads one already-selected semantic prefix and the raw suffix of the same coherent cut. */
    public static PositionedResult readMaterialized(
            KafkaObjectCoherentProtocolSnapshotV1 captured,
            KafkaObjectMaterializedPrefixV1 prefix,
            Sources sources,
            long offset,
            int maximumBytes,
            int hardMaximumBytes,
            boolean minOneMessage,
            KafkaReadIsolationV1 isolation,
            Cursor cursor)
            throws IOException {
        var coverage = prefix.coverage();
        var prefixEnd = coverage.exclusiveEnd();
        long isolationEnd = captured.root().readUpperBound(isolation);
        if (offset >= prefixEnd || offset >= isolationEnd || maximumBytes == 0) {
            return readPositioned(
                    captured, sources, offset, maximumBytes, hardMaximumBytes, minOneMessage, isolation, cursor);
        }
        requireReadableOffset(captured, offset);
        if (maximumBytes < 0 || hardMaximumBytes <= 0) {
            throw new IllegalArgumentException("invalid native Object WAL Fetch bytes");
        }
        if (coverage.inclusiveStart() > offset
                || prefixEnd > captured.root().frontiers().durableEndOffset()
                || prefix.rawFallbackEquivalent()) {
            throw new IOException("selected native Object WAL prefix differs from its coherent cut");
        }
        int soft = Math.min(maximumBytes, hardMaximumBytes);
        var selected = prefix.read(offset, isolationEnd, soft);
        var batches = new ArrayList<Batch>(selected.batches().size());
        int bytes = 0;
        boolean tailAllowed = selected.canReadTail();
        for (var batch : selected.batches()) {
            int length = batch.rawAssignedRecordBatch().length();
            if (length > hardMaximumBytes - bytes || length > soft - bytes && !(batches.isEmpty() && minOneMessage)) {
                tailAllowed = false;
                break;
            }
            batches.add(new Batch(batch.startOffset(), batch.endOffsetExclusive(), batch.rawAssignedRecordBatch()));
            bytes = Math.addExact(bytes, length);
        }
        Optional<Cursor> next = Optional.empty();
        if (tailAllowed && bytes < soft) {
            var tail = readPositioned(
                    captured,
                    sources,
                    prefixEnd,
                    soft - bytes,
                    hardMaximumBytes - bytes,
                    batches.isEmpty() && minOneMessage,
                    isolation,
                    cursor);
            batches.addAll(tail.result().batches());
            bytes = Math.addExact(bytes, tail.result().bytes());
            next = tail.nextCursor();
        }
        long returnedEnd =
                batches.isEmpty() ? offset : batches.get(batches.size() - 1).endOffsetExclusive();
        return new PositionedResult(
                new Result(batches, abortedTransactions(captured, isolation, batches, offset, returnedEnd), bytes),
                next);
    }

    private static void requireReadableOffset(KafkaObjectCoherentProtocolSnapshotV1 captured, long offset) {
        var frontiers = captured.root().frontiers();
        if (offset < frontiers.trimStartOffset() || offset > frontiers.readableEndOffset()) {
            throw new IllegalArgumentException("native Object WAL Fetch is outside its captured readable prefix");
        }
    }

    private static List<CompletedTransactionV1> abortedTransactions(
            KafkaObjectCoherentProtocolSnapshotV1 captured,
            KafkaReadIsolationV1 isolation,
            List<Batch> batches,
            long offset,
            long returnedEnd) {
        return isolation == KafkaReadIsolationV1.READ_COMMITTED && !batches.isEmpty()
                ? captured.transactionState().abortedTransactions().stream()
                        .filter(txn -> txn.markerEndOffsetExclusive() > offset && txn.firstOffset() < returnedEnd)
                        .toList()
                : List.of();
    }

    private static int startIndex(
            KafkaPartitionProtocolStateV1 root,
            KafkaObjectActiveTailStateV1 tail,
            long offset,
            long end,
            Cursor cursor) {
        if (cursor != null
                && cursor.matches(root, offset)
                && cursor.locator() >= 0
                && cursor.locator() < tail.locators().size()) {
            var selected = tail.locators().get(cursor.locator());
            if (selected.startOffset() <= offset && offset < selected.endOffsetExclusive()) {
                return cursor.locator();
            }
        }
        return tail.floorIndex(offset, end);
    }

    private static final class Collect {
        private final List<Batch> batches = new ArrayList<>();
        private final long upper;
        private final int soft;
        private final int hard;
        private final boolean minOneMessage;
        private long next;
        private int bytes;
        private boolean stopped;

        private Collect(long start, long upper, int soft, int hard, boolean minOneMessage) {
            this.next = start;
            this.upper = upper;
            this.soft = soft;
            this.hard = hard;
            this.minOneMessage = minOneMessage;
        }

        private void accept(Nwg1ObjectReaderV1.VerifiedFrame frame, ByteBuffer payload) throws IOException {
            if (stopped || frame.coverage1() <= next || frame.coverage0() >= upper) {
                return;
            }
            if (frame.coverage0() > next || frame.coverage1() > upper) {
                throw new IOException("native Object WAL RecordBatch crosses the captured range");
            }
            int length = payload.remaining();
            if (length > hard - bytes || length > soft - bytes && !(batches.isEmpty() && minOneMessage)) {
                stopped = true;
                return;
            }
            byte[] copy = new byte[length];
            payload.get(copy);
            batches.add(new Batch(frame.coverage0(), frame.coverage1(), CanonicalBytes.copyOf(copy)));
            bytes = Math.addExact(bytes, length);
            next = frame.coverage1();
            stopped = bytes >= soft;
        }
    }
}

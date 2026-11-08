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
import com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1;
import com.nereusstream.storage.object.nwg1.Nwg1ObjectReaderV1;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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

    public record Result(List<Batch> batches, List<CompletedTransactionV1> abortedTransactions, int bytes) {
        public Result {
            batches = List.copyOf(batches);
            abortedTransactions = List.copyOf(abortedTransactions);
        }
    }

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
        Objects.requireNonNull(captured, "captured");
        Objects.requireNonNull(sources, "sources");
        Objects.requireNonNull(isolation, "isolation");
        if (maximumBytes < 0 || hardMaximumBytes <= 0) {
            throw new IllegalArgumentException("invalid native Object WAL Fetch bytes");
        }
        var frontiers = captured.root().frontiers();
        if (offset < frontiers.trimStartOffset() || offset > frontiers.readableEndOffset()) {
            throw new IllegalArgumentException("native Object WAL Fetch is outside its captured readable prefix");
        }
        long end = captured.root().readUpperBound(isolation);
        if (end <= offset || maximumBytes == 0) {
            return new Result(List.of(), List.of(), 0);
        }
        var collected =
                new Collect(offset, end, Math.min(maximumBytes, hardMaximumBytes), hardMaximumBytes, minOneMessage);
        for (var locator : captured.activeTail().locators()) {
            if (locator.endOffsetExclusive() <= collected.next) {
                continue;
            }
            if (locator.startOffset() >= end || collected.stopped) {
                break;
            }
            if (locator.startOffset() > collected.next) {
                throw new IOException("native Object WAL captured locator plan has a gap");
            }
            try (var extent = Objects.requireNonNull(sources.pin(locator), "pinned extent")) {
                extent.read(collected::accept);
            }
            if (collected.next < Math.min(locator.endOffsetExclusive(), end) && !collected.stopped) {
                throw new IOException("native Object WAL extent did not cover the captured offset");
            }
        }
        if (collected.next < end && !collected.stopped) {
            throw new IOException("native Object WAL captured prefix lacks a readable extent");
        }
        long returnedEnd = collected.next;
        var aborted = isolation == KafkaReadIsolationV1.READ_COMMITTED && !collected.batches.isEmpty()
                ? captured.transactionState().abortedTransactions().stream()
                        .filter(txn -> txn.markerEndOffsetExclusive() > offset && txn.firstOffset() < returnedEnd)
                        .toList()
                : List.<CompletedTransactionV1>of();
        return new Result(collected.batches, aborted, collected.bytes);
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

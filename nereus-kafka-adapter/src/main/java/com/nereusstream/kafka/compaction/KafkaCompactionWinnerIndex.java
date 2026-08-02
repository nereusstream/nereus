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

package com.nereusstream.kafka.compaction;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.objectstore.compacted.KafkaCompactionKeyEncodingV2;
import com.nereusstream.objectstore.staging.PrivateStagingSpillFile;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.TreeMap;

/**
 * Bounded KCK2 winner index backed by checksum-verified, strictly sorted staging runs.
 *
 * <p>The runs are execution-local scratch state. Finishing the index reduces them to a bounded
 * winner bitmap and closes every spill. A restarted durable task deterministically rebuilds the
 * bitmap from its frozen exact source set.
 */
final class KafkaCompactionWinnerIndex implements AutoCloseable {
    static final int DEFAULT_MERGE_FAN_IN = 16;

    private static final int RUN_MAGIC = 0x4b435352; // KCSR
    private static final int RUN_VERSION = 1;
    private static final int RUN_END = -1;
    private static final int RUN_BUFFER_BYTES = 64 << 10;
    private static final long KEY_ENTRY_OVERHEAD = 64;

    private final StagingFileManager stagingFiles;
    private final OffsetRange outputCoverage;
    private final OffsetRange decisionHorizon;
    private final long maxInMemoryKeyBytes;
    private final int maxKeyBytes;
    private final int mergeFanIn;
    private final long maxEntries;
    private final TreeMap<ByteKey, Long> current = new TreeMap<>();
    private final ArrayList<Run> runs = new ArrayList<>();
    private long currentBytes;
    private long peakInMemoryKeyBytes;
    private long acceptedEntries;
    private long createdRuns;
    private boolean finished;
    private boolean closed;

    KafkaCompactionWinnerIndex(
            StagingFileManager stagingFiles,
            OffsetRange outputCoverage,
            OffsetRange decisionHorizon,
            long maxInMemoryKeyBytes,
            int maxKeyBytes,
            int mergeFanIn,
            long maxEntries) {
        this.stagingFiles = Objects.requireNonNull(stagingFiles, "stagingFiles");
        this.outputCoverage = Objects.requireNonNull(outputCoverage, "outputCoverage");
        this.decisionHorizon = Objects.requireNonNull(decisionHorizon, "decisionHorizon");
        if (outputCoverage.isEmpty()
                || decisionHorizon.isEmpty()
                || outputCoverage.startOffset() != decisionHorizon.startOffset()
                || outputCoverage.endOffset() > decisionHorizon.endOffset()
                || decisionHorizon.recordCount() > Integer.MAX_VALUE
                || maxInMemoryKeyBytes <= 0
                || maxKeyBytes <= 0
                || maxKeyBytes > KafkaCompactionKeyEncodingV2.MAX_ENCODED_KEY_BYTES
                || mergeFanIn < 2
                || mergeFanIn > 64
                || maxEntries <= 0
                || decisionHorizon.recordCount() > maxEntries) {
            throw new IllegalArgumentException("invalid Kafka compaction winner-index limits");
        }
        this.maxInMemoryKeyBytes = maxInMemoryKeyBytes;
        this.maxKeyBytes = maxKeyBytes;
        this.mergeFanIn = mergeFanIn;
        this.maxEntries = maxEntries;
    }

    void add(byte[] encodedKey, long absoluteOffset) {
        ensureOpen();
        ByteKey key = new ByteKey(requireKey(encodedKey));
        if (!decisionHorizon.contains(absoluteOffset)) {
            throw new IllegalArgumentException("Kafka compaction winner offset is outside the horizon");
        }
        acceptedEntries = Math.addExact(acceptedEntries, 1);
        if (acceptedEntries > maxEntries) {
            throw new IllegalArgumentException("Kafka compaction winner index exceeded its entry limit");
        }
        Long previous = current.get(key);
        if (previous != null) {
            if (absoluteOffset > previous) {
                current.put(key, absoluteOffset);
            }
            return;
        }
        long entryBytes = Math.addExact(key.length(), KEY_ENTRY_OVERHEAD);
        if (!current.isEmpty() && entryBytes > maxInMemoryKeyBytes - currentBytes) {
            flushCurrent();
        }
        current.put(key, absoluteOffset);
        currentBytes = Math.addExact(currentBytes, entryBytes);
        peakInMemoryKeyBytes = Math.max(peakInMemoryKeyBytes, currentBytes);
        if (currentBytes >= maxInMemoryKeyBytes) {
            flushCurrent();
        }
    }

    Result finish() {
        ensureOpen();
        finished = true;
        BitSet winners = new BitSet(Math.toIntExact(outputCoverage.recordCount()));
        try {
            if (runs.isEmpty()) {
                current.forEach((key, offset) -> selectWinner(winners, offset));
            } else {
                flushCurrent();
                reduceRunsToFanIn();
                mergeRuns(List.copyOf(runs), (key, offset) -> selectWinner(winners, offset));
            }
            return new Result(winners, createdRuns, peakInMemoryKeyBytes);
        } finally {
            closeRuns(runs);
            runs.clear();
            current.clear();
            currentBytes = 0;
            closed = true;
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        closeRuns(runs);
        runs.clear();
        current.clear();
        currentBytes = 0;
    }

    private void selectWinner(BitSet winners, long absoluteOffset) {
        if (outputCoverage.contains(absoluteOffset)) {
            winners.set(Math.toIntExact(Math.subtractExact(absoluteOffset, outputCoverage.startOffset())));
        }
    }

    private void flushCurrent() {
        if (current.isEmpty()) {
            return;
        }
        RunWriter writer = new RunWriter();
        boolean success = false;
        try {
            current.forEach((key, offset) -> writer.write(new Entry(key, offset)));
            runs.add(writer.finish());
            createdRuns = Math.addExact(createdRuns, 1);
            success = true;
        } finally {
            if (!success) {
                writer.close();
            }
        }
        current.clear();
        currentBytes = 0;
        if (runs.size() >= mergeFanIn) {
            Run merged = mergeToRun(List.copyOf(runs));
            closeRuns(runs);
            runs.clear();
            runs.add(merged);
        }
    }

    private void reduceRunsToFanIn() {
        while (runs.size() > mergeFanIn) {
            List<Run> group = List.copyOf(runs.subList(0, mergeFanIn));
            Run merged = mergeToRun(group);
            closeRuns(group);
            runs.subList(0, mergeFanIn).clear();
            runs.add(0, merged);
        }
    }

    private Run mergeToRun(List<Run> inputs) {
        RunWriter output = new RunWriter();
        boolean success = false;
        try {
            mergeRuns(inputs, (key, offset) -> output.write(new Entry(key, offset)));
            Run result = output.finish();
            createdRuns = Math.addExact(createdRuns, 1);
            success = true;
            return result;
        } finally {
            if (!success) {
                output.close();
            }
        }
    }

    private void mergeRuns(List<Run> inputs, WinnerConsumer consumer) {
        ArrayList<RunReader> readers = new ArrayList<>(inputs.size());
        PriorityQueue<RunReader> queue = new PriorityQueue<>(Comparator.comparing(reader -> reader.current.key()));
        try {
            for (Run run : inputs) {
                RunReader reader = new RunReader(run);
                readers.add(reader);
                if (reader.current != null) {
                    queue.add(reader);
                }
            }
            while (!queue.isEmpty()) {
                ByteKey key = queue.peek().current.key();
                long winner = -1;
                while (!queue.isEmpty() && queue.peek().current.key().equals(key)) {
                    RunReader reader = queue.remove();
                    winner = Math.max(winner, reader.current.absoluteOffset());
                    reader.advance();
                    if (reader.current != null) {
                        queue.add(reader);
                    }
                }
                if (winner < 0) {
                    throw invariant("Kafka compaction spill merge produced a negative winner", null);
                }
                consumer.accept(key, winner);
            }
        } finally {
            readers.forEach(RunReader::close);
        }
    }

    private final class RunWriter implements AutoCloseable {
        private final PrivateStagingSpillFile file = stagingFiles.createSpill("kafka-winner");
        private final DataOutputStream output;
        private ByteKey previous;
        private long entries;
        private boolean complete;

        private RunWriter() {
            try {
                output = new DataOutputStream(new BufferedOutputStream(file.outputStream(), RUN_BUFFER_BYTES));
                output.writeInt(RUN_MAGIC);
                output.writeInt(RUN_VERSION);
            } catch (IOException failure) {
                file.close();
                throw storageFailure("initialize Kafka compaction spill run", failure);
            }
        }

        private void write(Entry entry) {
            if (complete || previous != null && previous.compareTo(entry.key()) >= 0) {
                throw invariant("Kafka compaction spill run is not strictly key ordered", null);
            }
            try {
                output.writeInt(entry.key().length());
                output.write(entry.key().bytes);
                output.writeLong(entry.absoluteOffset());
                entries = Math.addExact(entries, 1);
                previous = entry.key();
            } catch (IOException failure) {
                throw storageFailure("write Kafka compaction spill run", failure);
            }
        }

        private Run finish() {
            if (complete) {
                throw new IllegalStateException("Kafka compaction spill writer is already finished");
            }
            try {
                output.writeInt(RUN_END);
                output.writeLong(entries);
                output.close();
                file.seal();
                complete = true;
                return new Run(file, entries);
            } catch (IOException failure) {
                throw storageFailure("finish Kafka compaction spill run", failure);
            }
        }

        @Override
        public void close() {
            if (complete) {
                return;
            }
            try {
                output.close();
            } catch (IOException ignored) {
            }
            file.close();
            complete = true;
        }
    }

    private final class RunReader implements AutoCloseable {
        private final Run run;
        private DataInputStream input;
        private ByteKey previous;
        private Entry current;
        private long entries;
        private boolean terminal;

        private RunReader(Run run) {
            this.run = run;
            try {
                input = new DataInputStream(
                        new BufferedInputStream(run.file().openVerifiedInputStream(), RUN_BUFFER_BYTES));
                if (input.readInt() != RUN_MAGIC || input.readInt() != RUN_VERSION) {
                    throw new IOException("Kafka compaction spill run header is invalid");
                }
                advance();
            } catch (IOException | RuntimeException failure) {
                close();
                throw storageFailure("open Kafka compaction spill run", failure);
            }
        }

        private void advance() {
            if (terminal) {
                current = null;
                return;
            }
            try {
                int keyLength = input.readInt();
                if (keyLength == RUN_END) {
                    long declaredEntries = input.readLong();
                    if (declaredEntries != entries || declaredEntries != run.entries()) {
                        throw new IOException("Kafka compaction spill run count is inconsistent");
                    }
                    if (input.read() != -1) {
                        throw new IOException("Kafka compaction spill run has trailing bytes");
                    }
                    terminal = true;
                    current = null;
                    return;
                }
                if (keyLength <= 0 || keyLength > maxKeyBytes) {
                    throw new IOException("Kafka compaction spill key length is invalid");
                }
                byte[] keyBytes = input.readNBytes(keyLength);
                if (keyBytes.length != keyLength) {
                    throw new EOFException("Kafka compaction spill key is truncated");
                }
                ByteKey key = new ByteKey(requireKey(keyBytes));
                if (previous != null && previous.compareTo(key) >= 0) {
                    throw new IOException("Kafka compaction spill keys are not strictly ordered");
                }
                long absoluteOffset = input.readLong();
                if (!decisionHorizon.contains(absoluteOffset)) {
                    throw new IOException("Kafka compaction spill winner is outside the decision horizon");
                }
                entries = Math.addExact(entries, 1);
                if (entries > maxEntries) {
                    throw new IOException("Kafka compaction spill run exceeds its entry limit");
                }
                current = new Entry(key, absoluteOffset);
                previous = key;
            } catch (IOException | RuntimeException failure) {
                throw storageFailure("read Kafka compaction spill run", failure);
            }
        }

        @Override
        public void close() {
            if (input == null) {
                return;
            }
            try {
                input.close();
            } catch (IOException ignored) {
            }
            input = null;
        }
    }

    private byte[] requireKey(byte[] encodedKey) {
        byte[] key = Objects.requireNonNull(encodedKey, "encodedKey").clone();
        if (key.length <= 0 || key.length > maxKeyBytes || key[0] != KafkaCompactionKeyEncodingV2.KEYED_TAG) {
            throw new IllegalArgumentException("Kafka compaction winner requires a canonical KCK2 key");
        }
        return key;
    }

    private void ensureOpen() {
        if (finished || closed) {
            throw new IllegalStateException("Kafka compaction winner index is already finished");
        }
    }

    private static void closeRuns(List<Run> runs) {
        runs.forEach(run -> run.file().close());
    }

    private static NereusException storageFailure(String message, Throwable cause) {
        if (cause instanceof NereusException nereus) {
            return nereus;
        }
        return new NereusException(ErrorCode.OBJECT_READ_FAILED, true, message, cause);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    record Result(BitSet latestOffsets, long spillRunCount, long peakInMemoryKeyBytes) {
        Result {
            latestOffsets = (BitSet)
                    Objects.requireNonNull(latestOffsets, "latestOffsets").clone();
            if (spillRunCount < 0 || peakInMemoryKeyBytes < 0) {
                throw new IllegalArgumentException("invalid Kafka compaction winner-index result");
            }
        }

        @Override
        public BitSet latestOffsets() {
            return (BitSet) latestOffsets.clone();
        }
    }

    private record Run(PrivateStagingSpillFile file, long entries) {
        private Run {
            Objects.requireNonNull(file, "file");
            if (entries <= 0) {
                throw new IllegalArgumentException("Kafka compaction spill run cannot be empty");
            }
        }
    }

    private record Entry(ByteKey key, long absoluteOffset) {
        private Entry {
            Objects.requireNonNull(key, "key");
            if (absoluteOffset < 0) {
                throw new IllegalArgumentException("Kafka compaction winner offset must be non-negative");
            }
        }
    }

    @FunctionalInterface
    private interface WinnerConsumer {
        void accept(ByteKey key, long absoluteOffset);
    }

    private static final class ByteKey implements Comparable<ByteKey> {
        private final byte[] bytes;
        private final int hash;

        private ByteKey(byte[] bytes) {
            this.bytes = Objects.requireNonNull(bytes, "bytes").clone();
            this.hash = Arrays.hashCode(this.bytes);
        }

        private int length() {
            return bytes.length;
        }

        @Override
        public int compareTo(ByteKey other) {
            int common = Math.min(bytes.length, other.bytes.length);
            for (int index = 0; index < common; index++) {
                int comparison =
                        Integer.compare(Byte.toUnsignedInt(bytes[index]), Byte.toUnsignedInt(other.bytes[index]));
                if (comparison != 0) {
                    return comparison;
                }
            }
            return Integer.compare(bytes.length, other.bytes.length);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ByteKey key && Arrays.equals(bytes, key.bytes);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}

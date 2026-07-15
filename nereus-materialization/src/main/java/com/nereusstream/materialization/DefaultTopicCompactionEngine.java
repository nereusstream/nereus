/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.metadata.oxia.records.TaskFailureClass;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV1;
import com.nereusstream.objectstore.compacted.TopicCompactionKeyEncodingV1;
import com.nereusstream.objectstore.staging.PrivateStagingSpillFile;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

/** Sorted-spill two-pass topic compaction with a bounded survivor bitmap and exact source replay. */
public final class DefaultTopicCompactionEngine implements TopicCompactionEngine {
    public static final long MIN_IN_MEMORY_KEY_BYTES = 64L << 10;
    public static final long MAX_IN_MEMORY_KEY_BYTES = 256L << 20;
    public static final int MAX_KEY_BYTES = CompactedObjectFormatV1.MAX_OPTIONAL_BINARY_BYTES - 1;
    public static final int MIN_MERGE_FAN_IN = 2;
    public static final int MAX_MERGE_FAN_IN = 64;

    private static final int RUN_MAGIC = 0x4e544352; // NTCR
    private static final int RUN_VERSION = 1;
    private static final int RUN_END = -1;
    private static final int RUN_BUFFER_BYTES = 64 << 10;
    private static final long KEY_ENTRY_OVERHEAD = 80;

    private final StagingFileManager stagingFiles;
    private final long maxInMemoryKeyBytes;
    private final int maxKeyBytes;
    private final int mergeFanIn;
    private final Executor executor;

    public DefaultTopicCompactionEngine(
            StagingFileManager stagingFiles,
            long maxInMemoryKeyBytes,
            int maxKeyBytes,
            int mergeFanIn,
            Executor executor) {
        this.stagingFiles = Objects.requireNonNull(stagingFiles, "stagingFiles");
        if (maxInMemoryKeyBytes < MIN_IN_MEMORY_KEY_BYTES
                || maxInMemoryKeyBytes > MAX_IN_MEMORY_KEY_BYTES) {
            throw new IllegalArgumentException("maxInMemoryKeyBytes must be in [64 KiB, 256 MiB]");
        }
        if (maxKeyBytes <= 0 || maxKeyBytes > MAX_KEY_BYTES) {
            throw new IllegalArgumentException("maxKeyBytes must be in [1, 1 MiB - 1]");
        }
        if (mergeFanIn < MIN_MERGE_FAN_IN || mergeFanIn > MAX_MERGE_FAN_IN) {
            throw new IllegalArgumentException("mergeFanIn must be in [2, 64]");
        }
        this.maxInMemoryKeyBytes = maxInMemoryKeyBytes;
        this.maxKeyBytes = maxKeyBytes;
        this.mergeFanIn = mergeFanIn;
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    @Override
    public CompletableFuture<TopicCompactionPlan> prepare(
            MaterializationTask task,
            ExactSourceRangeReader sourceReader,
            ReadOptions readOptions,
            TopicCompactionRegistry.Binding binding,
            long planningTimeMillis) {
        try {
            MaterializationTask exactTask = requireTask(task);
            ExactSourceRangeReader exactReader = Objects.requireNonNull(sourceReader, "sourceReader");
            ReadOptions exactOptions = Objects.requireNonNull(readOptions, "readOptions");
            TopicCompactionRegistry.Binding exactBinding = requireBinding(exactTask, binding);
            if (planningTimeMillis < 0) {
                throw new IllegalArgumentException("planningTimeMillis must be non-negative");
            }
            Collector collector = new Collector(exactTask, exactBinding, planningTimeMillis);
            ExactSourceBatchPublisher batches = new ExactSourceBatchPublisher(
                    exactTask, exactReader, exactOptions, executor);
            CompletableFuture<Void> passOne = collect(batches, collector);
            CompletableFuture<TopicCompactionPlan> result = passOne.thenApplyAsync(ignored -> {
                PreparedFacts facts = collector.finish();
                return new PreparedPlan(
                        exactTask,
                        exactReader,
                        exactOptions,
                        exactBinding,
                        facts,
                        maxKeyBytes,
                        executor);
            }, executor);
            result.whenComplete((ignored, failure) -> {
                batches.close();
                collector.close();
            });
            return result;
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static MaterializationTask requireTask(MaterializationTask task) {
        MaterializationTask exact = Objects.requireNonNull(task, "task");
        if (exact.taskKind() != TaskKind.TOPIC_KEY_COMPACTION
                || exact.view() != com.nereusstream.api.ReadView.TOPIC_COMPACTED
                || exact.sourceView() != com.nereusstream.api.ReadView.COMMITTED
                || !exact.policy().targetPhysicalFormat()
                        .equals(MaterializationPolicy.TOPIC_COMPACTED_FORMAT)
                || exact.policy().topicCompaction().isEmpty()) {
            throw new IllegalArgumentException(
                    "topic compaction requires a TOPIC_COMPACTED task over COMMITTED sources");
        }
        return exact;
    }

    private static TopicCompactionRegistry.Binding requireBinding(
            MaterializationTask task,
            TopicCompactionRegistry.Binding binding) {
        TopicCompactionRegistry.Binding exact = Objects.requireNonNull(binding, "binding");
        TopicCompactionSpec spec = task.policy().topicCompaction().orElseThrow();
        if (!exact.decoder().id().equals(spec.keyCodecId())
                || !exact.strategy().id().equals(spec.strategyId())
                || exact.strategy().version() != spec.strategyVersion()) {
            throw new IllegalArgumentException("topic compaction binding differs from durable task policy");
        }
        return exact;
    }

    private static CompletableFuture<Void> collect(
            ExactSourceBatchPublisher batches,
            Collector collector) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        batches.subscribe(new Flow.Subscriber<>() {
            private Flow.Subscription subscription;
            private boolean terminal;

            @Override
            public void onSubscribe(Flow.Subscription value) {
                if (subscription != null || terminal) {
                    value.cancel();
                    result.completeExceptionally(new IllegalStateException(
                            "pass-one exact source subscribed more than once"));
                    return;
                }
                subscription = value;
                value.request(1);
            }

            @Override
            public void onNext(ReadBatch item) {
                if (terminal) {
                    return;
                }
                try {
                    collector.accept(item);
                    subscription.request(1);
                } catch (Throwable failure) {
                    terminal = true;
                    subscription.cancel();
                    result.completeExceptionally(failure);
                }
            }

            @Override
            public void onError(Throwable failure) {
                if (!terminal) {
                    terminal = true;
                    result.completeExceptionally(failure);
                }
            }

            @Override
            public void onComplete() {
                if (!terminal) {
                    terminal = true;
                    result.complete(null);
                }
            }
        });
        return result;
    }

    private final class Collector implements AutoCloseable {
        private final MaterializationTask task;
        private final TopicCompactionRegistry.Binding binding;
        private final long planningTimeMillis;
        private final BitSet survivors;
        private final Map<ByteKey, Candidate> current = new HashMap<>();
        private final List<Run> runs = new ArrayList<>();
        private final FactDigest facts = new FactDigest();
        private long currentBytes;
        private int records;
        private boolean finished;
        private boolean closed;

        private Collector(
                MaterializationTask task,
                TopicCompactionRegistry.Binding binding,
                long planningTimeMillis) {
            this.task = task;
            this.binding = binding;
            this.planningTimeMillis = planningTimeMillis;
            this.survivors = new BitSet(Math.toIntExact(task.coverage().recordCount()));
        }

        private void accept(ReadBatch batch) {
            if (finished || closed) {
                throw new IllegalStateException("topic compaction collector is closed");
            }
            long offset = batch.range().startOffset();
            byte[] payload = batch.payload();
            Optional<CompactionRecord> decoded = decode(binding.decoder(), offset, payload);
            facts.add(offset, decoded);
            records = Math.addExact(records, 1);
            int relative = relativeOffset(task, offset);
            if (decoded.isEmpty()) {
                survivors.set(relative);
                return;
            }
            CompactionRecord record = validateDecoded(decoded.orElseThrow(), offset);
            byte[] rawKey = bytes(record.compactionKey());
            if (rawKey.length > maxKeyBytes) {
                throw execution(
                        TaskFailureClass.UNSUPPORTED_MAPPING,
                        ErrorCode.UNSUPPORTED_FORMAT,
                        false,
                        "decoded topic-compaction key exceeds the configured byte cap",
                        null);
            }
            ByteKey key = new ByteKey(bytes(TopicCompactionKeyEncodingV1.keyed(
                    ByteBuffer.wrap(rawKey))));
            Candidate candidate = Candidate.from(record);
            Candidate previous = current.get(key);
            if (previous == null) {
                long estimate = Math.addExact(key.bytes.length, KEY_ENTRY_OVERHEAD);
                if (!current.isEmpty() && currentBytes > maxInMemoryKeyBytes - estimate) {
                    flushCurrent();
                }
                current.put(key, candidate);
                currentBytes = Math.addExact(currentBytes, estimate);
            } else if (candidate.streamOffset > previous.streamOffset) {
                current.put(key, candidate);
            } else if (candidate.streamOffset == previous.streamOffset && !candidate.equals(previous)) {
                throw invariant("same compaction key/offset decoded to conflicting facts", null);
            }
            if (currentBytes > maxInMemoryKeyBytes) {
                flushCurrent();
            }
        }

        private PreparedFacts finish() {
            if (finished || closed) {
                throw new IllegalStateException("topic compaction collector was already finished");
            }
            finished = true;
            try {
                if (records != task.coverage().recordCount()) {
                    throw invariant("topic compaction pass one did not scan dense task coverage", null);
                }
                if (runs.isEmpty()) {
                    current.forEach(this::selectWinner);
                    current.clear();
                    currentBytes = 0;
                } else {
                    flushCurrent();
                    reduceRunsToFanIn();
                    mergeRuns(List.copyOf(runs), this::selectWinner);
                }
                return new PreparedFacts(
                        (BitSet) survivors.clone(),
                        survivors.cardinality(),
                        facts.finish());
            } finally {
                closeRuns(runs);
                runs.clear();
                current.clear();
                currentBytes = 0;
            }
        }

        private void selectWinner(ByteKey key, Candidate candidate) {
            boolean retain = candidate.disposition == CompactionDisposition.VALUE;
            if (!retain) {
                CompactionRecord tombstone = candidate.toRecord(decodedKey(key));
                try {
                    retain = binding.strategy().retainTombstone(tombstone, planningTimeMillis);
                } catch (Throwable failure) {
                    throw execution(
                            TaskFailureClass.UNSUPPORTED_MAPPING,
                            ErrorCode.UNSUPPORTED_FORMAT,
                            false,
                            "topic-compaction strategy rejected the durable task mapping",
                            failure);
                }
            }
            if (retain) {
                survivors.set(relativeOffset(task, candidate.streamOffset));
            }
        }

        private void flushCurrent() {
            if (current.isEmpty()) {
                return;
            }
            List<KeyedCandidate> entries = current.entrySet().stream()
                    .map(entry -> new KeyedCandidate(entry.getKey(), entry.getValue()))
                    .sorted(Comparator.comparing(KeyedCandidate::key))
                    .toList();
            Run run = writeRun(entries);
            runs.add(run);
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
    }

    private Run mergeToRun(List<Run> inputs) {
        RunWriter output = new RunWriter();
        boolean success = false;
        try {
            mergeRuns(inputs, (key, candidate) ->
                    output.write(new KeyedCandidate(key, candidate)));
            Run result = output.finish();
            success = true;
            return result;
        } finally {
            if (!success) {
                output.close();
            }
        }
    }

    private void mergeRuns(List<Run> inputs, WinnerConsumer consumer) {
        List<RunReader> readers = new ArrayList<>(inputs.size());
        PriorityQueue<RunReader> queue = new PriorityQueue<>(Comparator.comparing(
                reader -> reader.current.key));
        try {
            for (Run run : inputs) {
                RunReader reader = new RunReader(run);
                readers.add(reader);
                if (reader.current != null) {
                    queue.add(reader);
                }
            }
            while (!queue.isEmpty()) {
                ByteKey key = queue.peek().current.key;
                Candidate winner = null;
                while (!queue.isEmpty() && queue.peek().current.key.equals(key)) {
                    RunReader reader = queue.remove();
                    Candidate candidate = reader.current.candidate;
                    if (winner == null || candidate.streamOffset > winner.streamOffset) {
                        winner = candidate;
                    } else if (candidate.streamOffset == winner.streamOffset
                            && !candidate.equals(winner)) {
                        throw invariant("spill merge found conflicting same-offset facts", null);
                    }
                    reader.advance();
                    if (reader.current != null) {
                        queue.add(reader);
                    }
                }
                consumer.accept(key, Objects.requireNonNull(winner, "winner"));
            }
        } finally {
            readers.forEach(RunReader::close);
        }
    }

    private Run writeRun(List<KeyedCandidate> entries) {
        RunWriter writer = new RunWriter();
        boolean success = false;
        try {
            entries.forEach(writer::write);
            Run result = writer.finish();
            success = true;
            return result;
        } finally {
            if (!success) {
                writer.close();
            }
        }
    }

    private final class RunWriter implements AutoCloseable {
        private final PrivateStagingSpillFile file = stagingFiles.createSpill("topic-run");
        private final DataOutputStream output;
        private ByteKey previous;
        private long entries;
        private boolean finished;

        private RunWriter() {
            try {
                output = new DataOutputStream(new BufferedOutputStream(
                        file.outputStream(), RUN_BUFFER_BYTES));
                output.writeInt(RUN_MAGIC);
                output.writeInt(RUN_VERSION);
            } catch (IOException failure) {
                file.close();
                throw storageFailure("initialize topic-compaction spill run", failure);
            }
        }

        private void write(KeyedCandidate entry) {
            if (finished || previous != null && previous.compareTo(entry.key) >= 0) {
                throw invariant("topic-compaction spill run is not strictly key ordered", null);
            }
            try {
                byte[] key = entry.key.bytes;
                output.writeInt(key.length);
                output.write(key);
                output.writeLong(entry.candidate.streamOffset);
                output.writeInt(entry.candidate.disposition.wireId());
                writeOptionalLong(output, entry.candidate.publishTimeMillis);
                writeOptionalLong(output, entry.candidate.eventTimeMillis);
                entries = Math.addExact(entries, 1);
                previous = entry.key;
            } catch (IOException failure) {
                throw storageFailure("write topic-compaction spill run", failure);
            }
        }

        private Run finish() {
            if (finished) {
                throw new IllegalStateException("spill run writer was already finished");
            }
            try {
                output.writeInt(RUN_END);
                output.writeLong(entries);
                output.close();
                file.seal();
                finished = true;
                return new Run(file, entries);
            } catch (IOException failure) {
                throw storageFailure("finish topic-compaction spill run", failure);
            }
        }

        @Override
        public void close() {
            if (!finished) {
                try {
                    output.close();
                } catch (IOException ignored) {
                }
                file.close();
            }
        }
    }

    private final class RunReader implements AutoCloseable {
        private final Run run;
        private final DataInputStream input;
        private ByteKey previous;
        private KeyedCandidate current;
        private long entries;
        private boolean terminal;

        private RunReader(Run run) {
            this.run = run;
            try {
                this.input = new DataInputStream(new BufferedInputStream(
                        run.file.openVerifiedInputStream(), RUN_BUFFER_BYTES));
                if (input.readInt() != RUN_MAGIC || input.readInt() != RUN_VERSION) {
                    throw new IOException("topic-compaction spill run header is invalid");
                }
                advance();
            } catch (IOException failure) {
                throw storageFailure("open topic-compaction spill run", failure);
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
                    if (declaredEntries != entries || declaredEntries != run.entries) {
                        throw new IOException("topic-compaction spill run count is inconsistent");
                    }
                    if (input.read() != -1) {
                        throw new IOException("topic-compaction spill run has trailing bytes");
                    }
                    terminal = true;
                    current = null;
                    return;
                }
                if (keyLength <= 1 || keyLength > maxKeyBytes + 1) {
                    throw new IOException("topic-compaction spill key length is invalid");
                }
                byte[] keyBytes = input.readNBytes(keyLength);
                if (keyBytes.length != keyLength) {
                    throw new EOFException("topic-compaction spill key is truncated");
                }
                ByteKey key = new ByteKey(keyBytes);
                TopicCompactionKeyEncodingV1.DecodedKey decoded =
                        TopicCompactionKeyEncodingV1.decode(ByteBuffer.wrap(keyBytes));
                if (!(decoded instanceof TopicCompactionKeyEncodingV1.DecodedKey.Keyed)) {
                    throw new IOException("spill run contains an unkeyed namespace entry");
                }
                if (previous != null && previous.compareTo(key) >= 0) {
                    throw new IOException("topic-compaction spill keys are not strictly ordered");
                }
                long offset = input.readLong();
                CompactionDisposition disposition = CompactionDisposition.fromWireId(input.readInt());
                OptionalLong publish = readOptionalLong(input);
                OptionalLong event = readOptionalLong(input);
                if (offset < 0) {
                    throw new IOException("topic-compaction spill offset is negative");
                }
                entries = Math.addExact(entries, 1);
                if (entries > taskRecordHardLimit()) {
                    throw new IOException("topic-compaction spill run exceeds the task record hard limit");
                }
                current = new KeyedCandidate(
                        key, new Candidate(offset, disposition, publish, event));
                previous = key;
            } catch (IOException | RuntimeException failure) {
                throw storageFailure("read topic-compaction spill run", failure);
            }
        }

        @Override
        public void close() {
            try {
                input.close();
            } catch (IOException ignored) {
            }
        }
    }

    static Optional<CompactionRecord> decode(
            TopicCompactionDecoder decoder,
            long offset,
            byte[] payload) {
        try {
            return Objects.requireNonNull(
                    decoder.decode(offset, ByteBuffer.wrap(payload).asReadOnlyBuffer()),
                    "topic-compaction decoder result");
        } catch (MaterializationExecutionException failure) {
            throw failure;
        } catch (Throwable failure) {
            throw execution(
                    TaskFailureClass.UNSUPPORTED_MAPPING,
                    ErrorCode.UNSUPPORTED_FORMAT,
                    false,
                    "topic-compaction decoder cannot interpret the frozen payload mapping",
                    failure);
        }
    }

    static CompactionRecord validateDecoded(CompactionRecord record, long suppliedOffset) {
        CompactionRecord exact = Objects.requireNonNull(record, "record");
        if (exact.streamOffset() != suppliedOffset) {
            throw execution(
                    TaskFailureClass.OUTPUT_INVARIANT,
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "topic-compaction decoder changed the supplied stream offset",
                    null);
        }
        return exact;
    }

    static int relativeOffset(MaterializationTask task, long offset) {
        if (!task.coverage().contains(offset)) {
            throw invariant("topic-compaction offset is outside task coverage", null);
        }
        return Math.toIntExact(Math.subtractExact(offset, task.coverage().startOffset()));
    }

    static byte[] bytes(ByteBuffer value) {
        ByteBuffer source = value.asReadOnlyBuffer();
        byte[] bytes = new byte[source.remaining()];
        source.get(bytes);
        return bytes;
    }

    private static ByteBuffer decodedKey(ByteKey encoded) {
        TopicCompactionKeyEncodingV1.DecodedKey decoded =
                TopicCompactionKeyEncodingV1.decode(ByteBuffer.wrap(encoded.bytes));
        if (!(decoded instanceof TopicCompactionKeyEncodingV1.DecodedKey.Keyed keyed)) {
            throw invariant("spill winner is not in the keyed namespace", null);
        }
        return keyed.decodedKey();
    }

    private static void writeOptionalLong(DataOutputStream output, OptionalLong value)
            throws IOException {
        output.writeBoolean(value.isPresent());
        if (value.isPresent()) {
            output.writeLong(value.getAsLong());
        }
    }

    private static OptionalLong readOptionalLong(DataInputStream input) throws IOException {
        return input.readBoolean() ? OptionalLong.of(input.readLong()) : OptionalLong.empty();
    }

    private static long taskRecordHardLimit() {
        return MaterializationPolicy.MAX_RANGE_RECORDS;
    }

    private static void closeRuns(List<Run> runs) {
        runs.forEach(run -> run.file.close());
    }

    private static NereusException storageFailure(String message, Throwable cause) {
        if (cause instanceof NereusException nereus) {
            return nereus;
        }
        return new NereusException(ErrorCode.OBJECT_READ_FAILED, true, message, cause);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    private static MaterializationExecutionException execution(
            TaskFailureClass failureClass,
            ErrorCode code,
            boolean retriable,
            String message,
            Throwable cause) {
        return new MaterializationExecutionException(
                failureClass, code, retriable, message, cause);
    }

    private record PreparedFacts(
            BitSet survivors,
            int outputRecordCount,
            Checksum factSha256) {
        private PreparedFacts {
            survivors = (BitSet) survivors.clone();
            Objects.requireNonNull(factSha256, "factSha256");
            if (outputRecordCount < 0 || outputRecordCount != survivors.cardinality()) {
                throw new IllegalArgumentException("topic-compaction survivor count is inconsistent");
            }
        }

        @Override
        public BitSet survivors() {
            return (BitSet) survivors.clone();
        }
    }

    private record Run(PrivateStagingSpillFile file, long entries) {
        private Run {
            Objects.requireNonNull(file, "file");
            if (entries <= 0) {
                throw new IllegalArgumentException("spill run must contain at least one entry");
            }
        }
    }

    private record KeyedCandidate(ByteKey key, Candidate candidate) {
        private KeyedCandidate {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(candidate, "candidate");
        }
    }

    private record Candidate(
            long streamOffset,
            CompactionDisposition disposition,
            OptionalLong publishTimeMillis,
            OptionalLong eventTimeMillis) {
        private Candidate {
            if (streamOffset < 0) {
                throw new IllegalArgumentException("candidate offset must be non-negative");
            }
            Objects.requireNonNull(disposition, "disposition");
            publishTimeMillis = Objects.requireNonNull(publishTimeMillis, "publishTimeMillis");
            eventTimeMillis = Objects.requireNonNull(eventTimeMillis, "eventTimeMillis");
        }

        private static Candidate from(CompactionRecord record) {
            return new Candidate(
                    record.streamOffset(),
                    record.disposition(),
                    record.publishTimeMillis(),
                    record.eventTimeMillis());
        }

        private CompactionRecord toRecord(ByteBuffer decodedKey) {
            return new CompactionRecord(
                    streamOffset,
                    decodedKey,
                    disposition,
                    publishTimeMillis,
                    eventTimeMillis);
        }
    }

    private static final class ByteKey implements Comparable<ByteKey> {
        private final byte[] bytes;
        private final int hash;

        private ByteKey(byte[] bytes) {
            this.bytes = Objects.requireNonNull(bytes, "bytes").clone();
            this.hash = Arrays.hashCode(this.bytes);
        }

        @Override
        public int compareTo(ByteKey other) {
            int length = Math.min(bytes.length, other.bytes.length);
            for (int index = 0; index < length; index++) {
                int compared = Integer.compare(bytes[index] & 0xff, other.bytes[index] & 0xff);
                if (compared != 0) {
                    return compared;
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

    static final class FactDigest {
        private final MessageDigest digest = newDigest();
        private boolean finished;

        void add(long suppliedOffset, Optional<CompactionRecord> decoded) {
            if (finished) {
                throw new IllegalStateException("topic-compaction fact digest is sealed");
            }
            updateLong(digest, suppliedOffset);
            if (decoded.isEmpty()) {
                digest.update((byte) 0);
                return;
            }
            CompactionRecord record = validateDecoded(decoded.orElseThrow(), suppliedOffset);
            digest.update((byte) 1);
            byte[] key = bytes(record.compactionKey());
            updateInt(digest, key.length);
            digest.update(key);
            updateInt(digest, record.disposition().wireId());
            updateOptionalLong(digest, record.publishTimeMillis());
            updateOptionalLong(digest, record.eventTimeMillis());
        }

        Checksum finish() {
            if (finished) {
                throw new IllegalStateException("topic-compaction fact digest is already sealed");
            }
            finished = true;
            return new Checksum(
                    ChecksumType.SHA256, HexFormat.of().formatHex(digest.digest()));
        }

        private static void updateInt(MessageDigest digest, int value) {
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
        }

        private static void updateLong(MessageDigest digest, long value) {
            digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
        }

        private static void updateOptionalLong(MessageDigest digest, OptionalLong value) {
            digest.update((byte) (value.isPresent() ? 1 : 0));
            if (value.isPresent()) {
                updateLong(digest, value.getAsLong());
            }
        }
    }

    private static MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    @FunctionalInterface
    private interface WinnerConsumer {
        void accept(ByteKey key, Candidate candidate);
    }

    private static final class PreparedPlan implements TopicCompactionPlan {
        private final MaterializationTask task;
        private final ExactSourceRangeReader reader;
        private final ReadOptions options;
        private final TopicCompactionRegistry.Binding binding;
        private final PreparedFacts facts;
        private final int maxKeyBytes;
        private final Executor executor;
        private final AtomicBoolean subscribed = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();
        private volatile TopicCompactionRowPublisher active;

        private PreparedPlan(
                MaterializationTask task,
                ExactSourceRangeReader reader,
                ReadOptions options,
                TopicCompactionRegistry.Binding binding,
                PreparedFacts facts,
                int maxKeyBytes,
                Executor executor) {
            this.task = task;
            this.reader = reader;
            this.options = options;
            this.binding = binding;
            this.facts = facts;
            this.maxKeyBytes = maxKeyBytes;
            this.executor = executor;
        }

        @Override
        public int outputRecordCount() {
            return facts.outputRecordCount();
        }

        @Override
        public void subscribe(
                Flow.Subscriber<? super com.nereusstream.objectstore.compacted.CompactedObjectRow> subscriber) {
            Objects.requireNonNull(subscriber, "subscriber");
            if (!subscribed.compareAndSet(false, true) || closed.get()) {
                subscriber.onSubscribe(new Flow.Subscription() {
                    @Override public void request(long count) { }
                    @Override public void cancel() { }
                });
                subscriber.onError(new IllegalStateException(
                        "topic-compaction plan permits one subscription before close"));
                return;
            }
            TopicCompactionRowPublisher publisher = new TopicCompactionRowPublisher(
                    task,
                    reader,
                    options,
                    binding,
                    facts.survivors(),
                    facts.outputRecordCount(),
                    facts.factSha256(),
                    maxKeyBytes,
                    executor);
            active = publisher;
            publisher.subscribe(subscriber);
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                TopicCompactionRowPublisher publisher = active;
                if (publisher != null) {
                    publisher.close();
                }
            }
        }
    }
}

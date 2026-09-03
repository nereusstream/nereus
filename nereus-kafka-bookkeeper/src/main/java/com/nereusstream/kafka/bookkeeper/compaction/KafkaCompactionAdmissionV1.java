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

package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.CompactionPlan;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Persisted per-Cell M5-B admission; every external resource is reserved before dispatch. */
public final class KafkaCompactionAdmissionV1 {
    private static final int MAGIC = 0x4b354144; // K5AD
    private static final int VERSION = 1;
    public static final int MAX_RESERVATIONS = 1024;
    public static final int MAX_STATE_BYTES = 512 * 1024;

    public enum Outcome {
        APPLIED_EXACT,
        EXISTING_EXACT,
        REJECTED_CAP,
        DEFINITIVELY_NOT_APPLIED,
        OUTCOME_UNKNOWN,
        CONFLICT
    }

    public record CellCaps(
            int maximumRunningTasks,
            long maximumDirtyBytes,
            long maximumInputBatches,
            long maximumRecords,
            long maximumDistinctKeys,
            long maximumKeyBytes,
            long maximumOutputBytes,
            long maximumIndexBytes,
            long maximumTransactions,
            long maximumTombstones,
            long maximumBacklogAgeMs,
            long maximumSpillBytes,
            long maximumProviderOperations,
            long maximumKmsOperations,
            long maximumMetadataOperations,
            long maximumResponseUnknowns) {
        public CellCaps {
            if (maximumRunningTasks <= 0
                    || maximumRunningTasks > MAX_RESERVATIONS
                    || maximumDirtyBytes <= 0
                    || maximumInputBatches <= 0
                    || maximumRecords <= 0
                    || maximumDistinctKeys <= 0
                    || maximumKeyBytes <= 0
                    || maximumOutputBytes <= 0
                    || maximumIndexBytes <= 0
                    || maximumTransactions <= 0
                    || maximumTombstones <= 0
                    || maximumBacklogAgeMs <= 0
                    || maximumSpillBytes <= 0
                    || maximumProviderOperations <= 0
                    || maximumKmsOperations <= 0
                    || maximumMetadataOperations <= 0
                    || maximumResponseUnknowns <= 0) {
                throw new IllegalArgumentException("M5-B Cell caps must be finite and positive");
            }
        }

        private List<Long> values() {
            return List.of(
                    maximumDirtyBytes,
                    maximumInputBatches,
                    maximumRecords,
                    maximumDistinctKeys,
                    maximumKeyBytes,
                    maximumOutputBytes,
                    maximumIndexBytes,
                    maximumTransactions,
                    maximumTombstones,
                    maximumBacklogAgeMs,
                    maximumSpillBytes,
                    maximumProviderOperations,
                    maximumKmsOperations,
                    maximumMetadataOperations,
                    maximumResponseUnknowns);
        }
    }

    public record Reservation(
            Sha256Digest compactionTaskIdSha256,
            long dirtyBytes,
            long inputBatches,
            long records,
            long distinctKeys,
            long keyBytes,
            long outputBytes,
            long indexBytes,
            long transactions,
            long tombstones,
            long backlogAgeMs,
            long spillBytes,
            long providerOperations,
            long kmsOperations,
            long metadataOperations,
            long responseUnknowns) {
        public Reservation {
            KafkaCompactionRecordsV1.requireDigest(compactionTaskIdSha256, "compactionTaskIdSha256");
            if (dirtyBytes < 0
                    || inputBatches < 0
                    || records < 0
                    || distinctKeys < 0
                    || keyBytes < 0
                    || outputBytes < 0
                    || indexBytes < 0
                    || transactions < 0
                    || tombstones < 0
                    || backlogAgeMs < 0
                    || spillBytes < 0
                    || providerOperations < 0
                    || kmsOperations < 0
                    || metadataOperations < 0
                    || responseUnknowns < 0
                    || dirtyBytes == 0
                    || inputBatches == 0
                    || providerOperations == 0
                    || metadataOperations == 0
                    || responseUnknowns == 0) {
                throw new IllegalArgumentException("M5-B reservation is empty or negative");
            }
        }

        private List<Long> values() {
            return List.of(
                    dirtyBytes,
                    inputBatches,
                    records,
                    distinctKeys,
                    keyBytes,
                    outputBytes,
                    indexBytes,
                    transactions,
                    tombstones,
                    backlogAgeMs,
                    spillBytes,
                    providerOperations,
                    kmsOperations,
                    metadataOperations,
                    responseUnknowns);
        }
    }

    public record State(long generation, CellCaps caps, List<Reservation> reservations) {
        public State {
            Objects.requireNonNull(caps, "caps");
            reservations = List.copyOf(Objects.requireNonNull(reservations, "reservations"));
            if (generation <= 0 || reservations.size() > caps.maximumRunningTasks()) {
                throw new IllegalArgumentException("M5-B admission generation/count is invalid");
            }
            List<Reservation> sorted = reservations.stream()
                    .sorted(Comparator.comparing(
                            value -> value.compactionTaskIdSha256().toHex()))
                    .toList();
            if (!reservations.equals(sorted)
                    || reservations.stream()
                                    .map(Reservation::compactionTaskIdSha256)
                                    .distinct()
                                    .count()
                            != reservations.size()
                    || !within(caps, totals(reservations))) {
                throw new IllegalArgumentException("M5-B persisted reservations are noncanonical or over cap");
            }
        }
    }

    public record Totals(int runningTasks, List<Long> summed, long maximumBacklogAgeMs) {}

    private final CanonicalControlMetadataStore metadata;
    private final String key;

    public KafkaCompactionAdmissionV1(CanonicalControlMetadataStore metadata, Sha256Digest protocolCellSha256) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        KafkaCompactionRecordsV1.requireDigest(protocolCellSha256, "protocolCellSha256");
        this.key = "v2/object-wal/cells/" + protocolCellSha256.toHex() + "/read-m5/kafka-compaction-admission";
    }

    public Outcome install(CellCaps caps) {
        return reconcileCreate(encode(new State(1, caps, List.of())));
    }

    public Outcome reserve(
            CompactionPlan plan,
            long estimatedOutputBytes,
            long estimatedIndexBytes,
            long backlogAgeMs,
            long spillBytes,
            long providerOperations,
            long kmsOperations,
            long metadataOperations,
            long responseUnknowns) {
        Reservation reservation = reservation(
                plan,
                estimatedOutputBytes,
                estimatedIndexBytes,
                backlogAgeMs,
                spillBytes,
                providerOperations,
                kmsOperations,
                metadataOperations,
                responseUnknowns);
        State current = read().orElseThrow(() -> new IllegalStateException("M5-B Cell caps are not installed"));
        Optional<Reservation> existing = current.reservations().stream()
                .filter(value -> value.compactionTaskIdSha256().equals(reservation.compactionTaskIdSha256()))
                .findFirst();
        if (existing.isPresent()) {
            return existing.orElseThrow().equals(reservation) ? Outcome.EXISTING_EXACT : Outcome.CONFLICT;
        }
        List<Reservation> candidates = new ArrayList<>(current.reservations());
        candidates.add(reservation);
        candidates.sort(
                Comparator.comparing(value -> value.compactionTaskIdSha256().toHex()));
        if (!within(current.caps(), totals(candidates))) {
            return Outcome.REJECTED_CAP;
        }
        return reconcileCas(current, new State(Math.addExact(current.generation(), 1), current.caps(), candidates));
    }

    public Outcome release(Sha256Digest compactionTaskIdSha256) {
        KafkaCompactionRecordsV1.requireDigest(compactionTaskIdSha256, "compactionTaskIdSha256");
        State current = read().orElseThrow(() -> new IllegalStateException("M5-B Cell caps are not installed"));
        List<Reservation> survivors = current.reservations().stream()
                .filter(value -> !value.compactionTaskIdSha256().equals(compactionTaskIdSha256))
                .toList();
        if (survivors.size() == current.reservations().size()) {
            return Outcome.EXISTING_EXACT;
        }
        return reconcileCas(current, new State(Math.addExact(current.generation(), 1), current.caps(), survivors));
    }

    public Optional<State> read() {
        return metadata.get(key).map(KafkaCompactionAdmissionV1::decode);
    }

    public static Reservation reservation(
            CompactionPlan plan,
            long estimatedOutputBytes,
            long estimatedIndexBytes,
            long backlogAgeMs,
            long spillBytes,
            long providerOperations,
            long kmsOperations,
            long metadataOperations,
            long responseUnknowns) {
        Objects.requireNonNull(plan, "plan");
        Sha256Digest materializationTask = M5MaterializationCodecV1.calculateTaskId(plan.sourceCut());
        Sha256Digest task = KafkaCompactionCanonicalV1.compactionTaskId(
                materializationTask, KafkaCompactionCanonicalV1.planRoot(plan));
        long dirtyBytes = plan.inputBatches().stream()
                .mapToLong(value -> value.canonicalBody().length())
                .reduce(0, Math::addExact);
        long records = 0;
        long tombstones = 0;
        for (var input : plan.inputBatches()) {
            var batch = KafkaRecordBatchCodecV1.parse(input.canonicalBody());
            records = Math.addExact(records, batch.records().size());
            tombstones = Math.addExact(
                    tombstones,
                    batch.records().stream().filter(value -> value.tombstone()).count());
        }
        long keyBytes = plan.keyProofs().stream()
                .mapToLong(value -> value.key().length())
                .reduce(0, Math::addExact);
        return new Reservation(
                task,
                dirtyBytes,
                plan.inputBatches().size(),
                records,
                plan.keyProofs().size(),
                keyBytes,
                estimatedOutputBytes,
                estimatedIndexBytes,
                plan.transactions().size(),
                tombstones,
                backlogAgeMs,
                spillBytes,
                providerOperations,
                kmsOperations,
                metadataOperations,
                responseUnknowns);
    }

    public static Totals totals(List<Reservation> reservations) {
        long[] sums = new long[15];
        long backlog = 0;
        for (Reservation value : reservations) {
            List<Long> fields = value.values();
            for (int index = 0; index < fields.size(); index++) {
                if (index == 9) {
                    backlog = Math.max(backlog, fields.get(index));
                } else {
                    sums[index] = Math.addExact(sums[index], fields.get(index));
                }
            }
        }
        List<Long> result = new ArrayList<>(sums.length);
        for (long value : sums) {
            result.add(value);
        }
        return new Totals(reservations.size(), List.copyOf(result), backlog);
    }

    private static boolean within(CellCaps caps, Totals totals) {
        if (totals.runningTasks() > caps.maximumRunningTasks()
                || totals.maximumBacklogAgeMs() > caps.maximumBacklogAgeMs()) {
            return false;
        }
        List<Long> limits = caps.values();
        for (int index = 0; index < limits.size(); index++) {
            if (index != 9 && totals.summed().get(index) > limits.get(index)) {
                return false;
            }
        }
        return true;
    }

    private Outcome reconcileCreate(CanonicalBytes candidate) {
        ControlMutationOutcome mutation = metadata.putIfAbsent(key, candidate);
        Optional<CanonicalBytes> observed = metadata.get(key);
        if (observed.isPresent() && observed.orElseThrow().equals(candidate)) {
            return mutation == ControlMutationOutcome.APPLIED ? Outcome.APPLIED_EXACT : Outcome.EXISTING_EXACT;
        }
        return observed.isEmpty() ? Outcome.OUTCOME_UNKNOWN : Outcome.CONFLICT;
    }

    private Outcome reconcileCas(State current, State successor) {
        CanonicalBytes expected = encode(current);
        CanonicalBytes candidate = encode(successor);
        ControlMutationOutcome mutation = metadata.compareAndSet(key, Optional.of(expected), candidate);
        Optional<CanonicalBytes> observed = metadata.get(key);
        if (observed.isPresent() && observed.orElseThrow().equals(candidate)) {
            return mutation == ControlMutationOutcome.APPLIED ? Outcome.APPLIED_EXACT : Outcome.EXISTING_EXACT;
        }
        if (observed.isPresent() && observed.orElseThrow().equals(expected)) {
            return mutation == ControlMutationOutcome.DEFINITIVE_CONFLICT
                    ? Outcome.CONFLICT
                    : Outcome.DEFINITIVELY_NOT_APPLIED;
        }
        return observed.isEmpty() ? Outcome.OUTCOME_UNKNOWN : Outcome.CONFLICT;
    }

    public static CanonicalBytes encode(State state) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(MAGIC);
                output.writeInt(VERSION);
                output.writeLong(state.generation());
                output.writeInt(state.caps().maximumRunningTasks());
                for (long value : state.caps().values()) {
                    output.writeLong(value);
                }
                output.writeInt(state.reservations().size());
                for (Reservation reservation : state.reservations()) {
                    output.write(reservation.compactionTaskIdSha256().bytes().toByteArray());
                    for (long value : reservation.values()) {
                        output.writeLong(value);
                    }
                }
            }
            if (bytes.size() > MAX_STATE_BYTES) {
                throw new IllegalArgumentException("M5-B admission state exceeds its byte cap");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory M5-B admission encoding failed", impossible);
        }
    }

    public static State decode(CanonicalBytes encoded) {
        if (encoded == null || encoded.isEmpty() || encoded.length() > MAX_STATE_BYTES) {
            throw new IllegalArgumentException("M5-B admission state length is outside its cap");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded.toByteArray()))) {
            if (input.readInt() != MAGIC || input.readInt() != VERSION) {
                throw new IllegalArgumentException("M5-B admission state magic/version differs");
            }
            long generation = input.readLong();
            int maximumTasks = input.readInt();
            long[] caps = readLongs(input, 15);
            CellCaps cellCaps = new CellCaps(
                    maximumTasks,
                    caps[0],
                    caps[1],
                    caps[2],
                    caps[3],
                    caps[4],
                    caps[5],
                    caps[6],
                    caps[7],
                    caps[8],
                    caps[9],
                    caps[10],
                    caps[11],
                    caps[12],
                    caps[13],
                    caps[14]);
            int count = input.readInt();
            if (count < 0 || count > MAX_RESERVATIONS) {
                throw new IllegalArgumentException("M5-B admission reservation count is outside its cap");
            }
            List<Reservation> reservations = new ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                byte[] digest = input.readNBytes(Sha256Digest.LENGTH);
                if (digest.length != Sha256Digest.LENGTH) {
                    throw new EOFException("truncated M5-B admission task digest");
                }
                long[] values = readLongs(input, 15);
                reservations.add(new Reservation(
                        Sha256Digest.copyOf(digest),
                        values[0],
                        values[1],
                        values[2],
                        values[3],
                        values[4],
                        values[5],
                        values[6],
                        values[7],
                        values[8],
                        values[9],
                        values[10],
                        values[11],
                        values[12],
                        values[13],
                        values[14]));
            }
            if (input.read() != -1) {
                throw new IllegalArgumentException("M5-B admission state has trailing bytes");
            }
            State result = new State(generation, cellCaps, reservations);
            if (!encode(result).equals(encoded)) {
                throw new IllegalArgumentException("M5-B admission state is not canonical");
            }
            return result;
        } catch (IOException error) {
            throw new IllegalArgumentException("invalid M5-B admission state", error);
        }
    }

    private static long[] readLongs(DataInputStream input, int count) throws IOException {
        long[] result = new long[count];
        for (int index = 0; index < count; index++) {
            result[index] = input.readLong();
        }
        return result;
    }
}

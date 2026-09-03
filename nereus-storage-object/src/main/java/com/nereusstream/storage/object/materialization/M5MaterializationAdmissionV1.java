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

package com.nereusstream.storage.object.materialization;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationPlan;
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

/** Persisted finite per-Cell materialization reservations. Exhaustion never evicts readable state. */
public final class M5MaterializationAdmissionV1 {
    private static final int MAGIC = 0x4d354144; // M5AD
    private static final int VERSION = 1;
    public static final int MAX_RESERVATIONS = 1024;
    public static final int MAX_STATE_BYTES = 256 * 1024;

    public enum Outcome {
        APPLIED_EXACT,
        EXISTING_EXACT,
        REJECTED_CAP,
        DEFINITIVELY_NOT_APPLIED,
        OUTCOME_UNKNOWN,
        CONFLICT
    }

    public record Caps(
            int maximumTasks,
            long maximumSourceBytes,
            long maximumOutputBytes,
            int maximumSourceMembers,
            int maximumOutputParts,
            int maximumIndexes,
            int maximumResponseUnknowns) {
        public Caps {
            if (maximumTasks <= 0
                    || maximumTasks > MAX_RESERVATIONS
                    || maximumSourceBytes <= 0
                    || maximumOutputBytes <= 0
                    || maximumSourceMembers <= 0
                    || maximumOutputParts <= 0
                    || maximumIndexes <= 0
                    || maximumResponseUnknowns <= 0) {
                throw new IllegalArgumentException("M5 admission caps must be finite and positive");
            }
        }
    }

    public record Reservation(
            Sha256Digest taskIdSha256,
            long sourceBytes,
            long outputBytes,
            int sourceMembers,
            int outputParts,
            int indexes,
            int responseUnknownSlots) {
        public Reservation {
            requireDigest(taskIdSha256, "taskIdSha256");
            if (sourceBytes <= 0
                    || outputBytes <= 0
                    || sourceMembers <= 0
                    || outputParts <= 0
                    || indexes < 0
                    || responseUnknownSlots <= 0) {
                throw new IllegalArgumentException("M5 materialization reservation is empty");
            }
        }
    }

    public record State(long generation, Caps caps, List<Reservation> reservations) {
        public State {
            Objects.requireNonNull(caps, "caps");
            reservations = List.copyOf(Objects.requireNonNull(reservations, "reservations"));
            if (generation <= 0 || reservations.size() > caps.maximumTasks()) {
                throw new IllegalArgumentException("M5 admission state generation/count is invalid");
            }
            List<Reservation> sorted = reservations.stream()
                    .sorted(Comparator.comparing(value -> value.taskIdSha256().toHex()))
                    .toList();
            if (!reservations.equals(sorted)
                    || reservations.stream()
                                    .map(Reservation::taskIdSha256)
                                    .distinct()
                                    .count()
                            != reservations.size()) {
                throw new IllegalArgumentException("M5 reservations are not sorted unique");
            }
            Totals totals = totals(reservations);
            if (!within(caps, totals)) {
                throw new IllegalArgumentException("M5 persisted reservations exceed their caps");
            }
        }
    }

    public record Totals(
            int tasks,
            long sourceBytes,
            long outputBytes,
            int sourceMembers,
            int outputParts,
            int indexes,
            int responseUnknowns) {}

    private final CanonicalControlMetadataStore metadata;
    private final String stateKey;

    public M5MaterializationAdmissionV1(CanonicalControlMetadataStore metadata, Sha256Digest protocolCellSha256) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        requireDigest(protocolCellSha256, "protocolCellSha256");
        stateKey = "v2/object-wal/cells/" + protocolCellSha256.toHex() + "/read-m5/materialization-admission";
    }

    public Outcome install(Caps caps) {
        return reconcileCreate(encode(new State(1, caps, List.of())));
    }

    public Outcome reserve(MaterializationPlan plan, long estimatedOutputBytes, int responseUnknownSlots) {
        Objects.requireNonNull(plan, "plan");
        M5MaterializationCodecV1.encodePlan(plan);
        long sourceBytes = plan.sourceCut().sources().stream()
                .mapToLong(value -> value.canonicalLength())
                .reduce(0, Math::addExact);
        Reservation reservation = new Reservation(
                plan.taskIdSha256(),
                sourceBytes,
                estimatedOutputBytes,
                plan.sourceCut().sources().size(),
                plan.outputParts().size(),
                plan.indexes().size(),
                responseUnknownSlots);
        State current = read().orElseThrow(() -> new IllegalStateException("M5 admission caps are not installed"));
        Optional<Reservation> existing = current.reservations().stream()
                .filter(value -> value.taskIdSha256().equals(plan.taskIdSha256()))
                .findFirst();
        if (existing.isPresent()) {
            return existing.orElseThrow().equals(reservation) ? Outcome.EXISTING_EXACT : Outcome.CONFLICT;
        }
        List<Reservation> candidates = new ArrayList<>(current.reservations());
        candidates.add(reservation);
        candidates.sort(Comparator.comparing(value -> value.taskIdSha256().toHex()));
        if (!within(current.caps(), totals(candidates))) {
            return Outcome.REJECTED_CAP;
        }
        State successor = new State(Math.addExact(current.generation(), 1), current.caps(), candidates);
        return reconcileCas(current, successor);
    }

    public Outcome release(Sha256Digest taskIdSha256) {
        requireDigest(taskIdSha256, "taskIdSha256");
        State current = read().orElseThrow(() -> new IllegalStateException("M5 admission caps are not installed"));
        List<Reservation> survivors = current.reservations().stream()
                .filter(value -> !value.taskIdSha256().equals(taskIdSha256))
                .toList();
        if (survivors.size() == current.reservations().size()) {
            return Outcome.EXISTING_EXACT;
        }
        return reconcileCas(current, new State(Math.addExact(current.generation(), 1), current.caps(), survivors));
    }

    public Optional<State> read() {
        return metadata.get(stateKey).map(M5MaterializationAdmissionV1::decode);
    }

    private Outcome reconcileCreate(CanonicalBytes candidate) {
        ControlMutationOutcome mutation = metadata.putIfAbsent(stateKey, candidate);
        Optional<CanonicalBytes> observed = metadata.get(stateKey);
        if (observed.isPresent() && observed.orElseThrow().equals(candidate)) {
            return mutation == ControlMutationOutcome.APPLIED ? Outcome.APPLIED_EXACT : Outcome.EXISTING_EXACT;
        }
        return observed.isEmpty() ? Outcome.OUTCOME_UNKNOWN : Outcome.CONFLICT;
    }

    private Outcome reconcileCas(State current, State successor) {
        CanonicalBytes expected = encode(current);
        CanonicalBytes candidate = encode(successor);
        ControlMutationOutcome mutation = metadata.compareAndSet(stateKey, Optional.of(expected), candidate);
        Optional<CanonicalBytes> observed = metadata.get(stateKey);
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
                Caps caps = state.caps();
                output.writeInt(caps.maximumTasks());
                output.writeLong(caps.maximumSourceBytes());
                output.writeLong(caps.maximumOutputBytes());
                output.writeInt(caps.maximumSourceMembers());
                output.writeInt(caps.maximumOutputParts());
                output.writeInt(caps.maximumIndexes());
                output.writeInt(caps.maximumResponseUnknowns());
                output.writeInt(state.reservations().size());
                for (Reservation value : state.reservations()) {
                    output.write(value.taskIdSha256().bytes().toByteArray());
                    output.writeLong(value.sourceBytes());
                    output.writeLong(value.outputBytes());
                    output.writeInt(value.sourceMembers());
                    output.writeInt(value.outputParts());
                    output.writeInt(value.indexes());
                    output.writeInt(value.responseUnknownSlots());
                }
            }
            if (bytes.size() <= 0 || bytes.size() > MAX_STATE_BYTES) {
                throw new IllegalArgumentException("M5 admission state exceeds its byte cap");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException error) {
            throw new IllegalStateException("in-memory M5 admission encoding failed", error);
        }
    }

    public static State decode(CanonicalBytes encoded) {
        if (encoded == null || encoded.isEmpty() || encoded.length() > MAX_STATE_BYTES) {
            throw new IllegalArgumentException("M5 admission state length is outside its cap");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded.toByteArray()))) {
            if (input.readInt() != MAGIC || input.readInt() != VERSION) {
                throw new IllegalArgumentException("M5 admission state magic/version differs");
            }
            long generation = input.readLong();
            Caps caps = new Caps(
                    input.readInt(),
                    input.readLong(),
                    input.readLong(),
                    input.readInt(),
                    input.readInt(),
                    input.readInt(),
                    input.readInt());
            int count = input.readInt();
            if (count < 0 || count > MAX_RESERVATIONS) {
                throw new IllegalArgumentException("M5 admission reservation count is outside its cap");
            }
            List<Reservation> reservations = new ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                byte[] digest = input.readNBytes(Sha256Digest.LENGTH);
                if (digest.length != Sha256Digest.LENGTH) {
                    throw new EOFException("truncated M5 admission task digest");
                }
                reservations.add(new Reservation(
                        Sha256Digest.copyOf(digest),
                        input.readLong(),
                        input.readLong(),
                        input.readInt(),
                        input.readInt(),
                        input.readInt(),
                        input.readInt()));
            }
            if (input.read() != -1) {
                throw new IllegalArgumentException("M5 admission state has trailing bytes");
            }
            State result = new State(generation, caps, reservations);
            if (!encode(result).equals(encoded)) {
                throw new IllegalArgumentException("M5 admission state is not canonical");
            }
            return result;
        } catch (IOException error) {
            throw new IllegalArgumentException("invalid M5 admission state", error);
        }
    }

    public static Totals totals(List<Reservation> reservations) {
        int tasks = reservations.size();
        long sourceBytes = 0;
        long outputBytes = 0;
        int sourceMembers = 0;
        int outputParts = 0;
        int indexes = 0;
        int unknowns = 0;
        for (Reservation value : reservations) {
            sourceBytes = Math.addExact(sourceBytes, value.sourceBytes());
            outputBytes = Math.addExact(outputBytes, value.outputBytes());
            sourceMembers = Math.addExact(sourceMembers, value.sourceMembers());
            outputParts = Math.addExact(outputParts, value.outputParts());
            indexes = Math.addExact(indexes, value.indexes());
            unknowns = Math.addExact(unknowns, value.responseUnknownSlots());
        }
        return new Totals(tasks, sourceBytes, outputBytes, sourceMembers, outputParts, indexes, unknowns);
    }

    private static boolean within(Caps caps, Totals value) {
        return value.tasks() <= caps.maximumTasks()
                && value.sourceBytes() <= caps.maximumSourceBytes()
                && value.outputBytes() <= caps.maximumOutputBytes()
                && value.sourceMembers() <= caps.maximumSourceMembers()
                && value.outputParts() <= caps.maximumOutputParts()
                && value.indexes() <= caps.maximumIndexes()
                && value.responseUnknowns() <= caps.maximumResponseUnknowns();
    }

    private static void requireDigest(Sha256Digest value, String label) {
        Objects.requireNonNull(value, label);
        if (value.isZero()) {
            throw new IllegalArgumentException(label + " is the zero digest");
        }
    }
}

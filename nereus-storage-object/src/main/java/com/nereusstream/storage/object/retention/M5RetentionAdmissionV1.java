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

package com.nereusstream.storage.object.retention;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Persisted finite M5-C admission state and typed capacity alerts for one Cell/Binding. */
public final class M5RetentionAdmissionV1 {
    private static final int MAGIC = 0x4d354341; // M5CA
    private static final int VERSION = 1;
    public static final int MAX_RESERVATIONS = 4_096;
    public static final int MAX_STATE_BYTES = 1024 * 1024;
    private static final int MAX_ALERT_TEXT_BYTES = 4_096;

    public enum LimitKindV1 {
        RESERVATIONS,
        RETAINED_SOURCE_COUNT,
        RETAINED_SOURCE_BYTES,
        RETAINED_SOURCE_MAX_AGE_MILLIS,
        ACTIVE_FULL_BATCHES,
        FULL_BATCH_BYTES,
        MEMBER_SCAN_COUNT,
        EXTERNALIZATION_UNKNOWNS,
        PERMANENT_BATCH_TOMBSTONE_COUNT,
        PERMANENT_BATCH_TOMBSTONE_BYTES,
        PULSAR_SELECTOR_COUNT,
        PULSAR_SELECTOR_BYTES,
        PULSAR_TOMBSTONE_COUNT,
        PULSAR_TOMBSTONE_BYTES,
        REFERENCE_ROWS,
        REFERENCE_PAGES,
        REFERENCE_BYTES,
        AUDIT_GRACE_BACKLOG,
        QUARANTINES
    }

    public enum Outcome {
        INSTALLED,
        APPLIED_EXACT,
        EXISTING_EXACT,
        REJECTED_CAP,
        DEFINITIVELY_NOT_APPLIED,
        RESPONSE_UNKNOWN,
        CONFLICT
    }

    /** Exactly one finite positive hard value for every closed limit kind. */
    public record Caps(Map<LimitKindV1, Long> hardLimits) {
        public Caps {
            hardLimits = exactMap(hardLimits, true, "hard limits");
            if (hardLimits.get(LimitKindV1.RESERVATIONS) > MAX_RESERVATIONS) {
                throw new IllegalArgumentException("reservation cap exceeds the M5-C hard maximum");
            }
        }

        public long hard(LimitKindV1 kind) {
            return hardLimits.get(Objects.requireNonNull(kind, "kind"));
        }
    }

    /** One deterministic reservation. Age uses max aggregation; every other limit uses exact sum. */
    public record Reservation(
            Sha256Digest reservationIdSha256,
            Sha256Digest oldestExactIdentitySha256,
            Map<LimitKindV1, Long> reserved,
            List<ReferenceKindV1> blockingReferenceKinds,
            Optional<String> quarantineReason) {
        public Reservation {
            requireDigest(reservationIdSha256, "reservationIdSha256");
            requireDigest(oldestExactIdentitySha256, "oldestExactIdentitySha256");
            reserved = exactMap(reserved, false, "reserved values");
            blockingReferenceKinds =
                    List.copyOf(Objects.requireNonNull(blockingReferenceKinds, "blockingReferenceKinds"));
            if (!blockingReferenceKinds.equals(
                            blockingReferenceKinds.stream().sorted().toList())
                    || blockingReferenceKinds.stream().distinct().count() != blockingReferenceKinds.size()) {
                throw new IllegalArgumentException("blocking reference kinds are not sorted unique");
            }
            quarantineReason = Objects.requireNonNull(quarantineReason, "quarantineReason");
            quarantineReason.ifPresent(reason -> requireText(reason, "quarantineReason", MAX_ALERT_TEXT_BYTES));
            if (reserved.values().stream().allMatch(value -> value == 0)) {
                throw new IllegalArgumentException("retention admission reservation is empty");
            }
            if (reserved.get(LimitKindV1.RESERVATIONS) != 1) {
                throw new IllegalArgumentException("each reservation must reserve exactly one reservation slot");
            }
            if ((reserved.get(LimitKindV1.QUARANTINES) > 0) != quarantineReason.isPresent()) {
                throw new IllegalArgumentException("quarantine usage and reason disagree");
            }
        }
    }

    /** Fully derived usage. It is persisted and independently recomputed on decode. */
    public record Usage(Map<LimitKindV1, Long> values) {
        public Usage {
            values = exactMap(values, false, "usage values");
        }

        public long value(LimitKindV1 kind) {
            return values.get(Objects.requireNonNull(kind, "kind"));
        }
    }

    public record State(
            Sha256Digest protocolCellSha256,
            BindingIdentity binding,
            long generation,
            Caps caps,
            List<Reservation> reservations,
            Usage usage) {
        public State {
            requireDigest(protocolCellSha256, "protocolCellSha256");
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(caps, "caps");
            reservations = List.copyOf(Objects.requireNonNull(reservations, "reservations"));
            Objects.requireNonNull(usage, "usage");
            if (generation <= 0 || reservations.size() > MAX_RESERVATIONS) {
                throw new IllegalArgumentException("retention admission generation/count is invalid");
            }
            Comparator<Reservation> order =
                    Comparator.comparing(value -> value.reservationIdSha256().toHex());
            if (!reservations.equals(reservations.stream().sorted(order).toList())
                    || reservations.stream()
                                    .map(Reservation::reservationIdSha256)
                                    .distinct()
                                    .count()
                            != reservations.size()) {
                throw new IllegalArgumentException("retention reservations are not sorted unique");
            }
            Usage calculated = calculateUsage(reservations);
            if (!calculated.equals(usage)) {
                throw new IllegalArgumentException("persisted retention usage differs from exact reservations");
            }
            Optional<LimitKindV1> exceeded = firstExceeded(caps, usage);
            if (exceeded.isPresent()) {
                throw new IllegalArgumentException("persisted retention state exceeds " + exceeded.orElseThrow());
            }
        }
    }

    public record AlertV1(
            Sha256Digest protocolCellSha256,
            BindingIdentity binding,
            LimitKindV1 limitKind,
            long currentValue,
            long reservedValue,
            long hardValue,
            Sha256Digest oldestExactIdentitySha256,
            long oldestAgeMillis,
            List<ReferenceKindV1> blockingReferenceKinds,
            Optional<String> quarantineReason) {
        public AlertV1 {
            requireDigest(protocolCellSha256, "protocolCellSha256");
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(limitKind, "limitKind");
            requireDigest(oldestExactIdentitySha256, "oldestExactIdentitySha256");
            blockingReferenceKinds =
                    List.copyOf(Objects.requireNonNull(blockingReferenceKinds, "blockingReferenceKinds"));
            quarantineReason = Objects.requireNonNull(quarantineReason, "quarantineReason");
            if (currentValue < 0 || reservedValue < 0 || hardValue <= 0 || oldestAgeMillis < 0) {
                throw new IllegalArgumentException("retention alert values are outside their domains");
            }
            if (!blockingReferenceKinds.equals(
                            blockingReferenceKinds.stream().sorted().toList())
                    || blockingReferenceKinds.stream().distinct().count() != blockingReferenceKinds.size()) {
                throw new IllegalArgumentException("alert blocking reference kinds are not sorted unique");
            }
            quarantineReason.ifPresent(reason -> requireText(reason, "quarantineReason", MAX_ALERT_TEXT_BYTES));
            long projected = limitKind == LimitKindV1.RETAINED_SOURCE_MAX_AGE_MILLIS
                    ? Math.max(currentValue, reservedValue)
                    : Math.addExact(currentValue, reservedValue);
            if (projected <= hardValue) {
                throw new IllegalArgumentException("retention alert does not exceed its hard value");
            }
            if (limitKind == LimitKindV1.QUARANTINES && quarantineReason.isEmpty()) {
                throw new IllegalArgumentException("quarantine-cap alert lacks its exact reason");
            }
        }
    }

    public record Result(Outcome outcome, Optional<State> exactState, Optional<AlertV1> alert) {
        public Result {
            Objects.requireNonNull(outcome, "outcome");
            exactState = Objects.requireNonNull(exactState, "exactState");
            alert = Objects.requireNonNull(alert, "alert");
            boolean exact = outcome == Outcome.INSTALLED
                    || outcome == Outcome.APPLIED_EXACT
                    || outcome == Outcome.EXISTING_EXACT;
            if (exact != exactState.isPresent() || (outcome == Outcome.REJECTED_CAP) != alert.isPresent()) {
                throw new IllegalArgumentException("retention admission result payload differs from its outcome");
            }
        }
    }

    private final ExactMetadataTransactionStoreV1 metadata;
    private final String stateKey;
    private final Sha256Digest protocolCellSha256;
    private final BindingIdentity binding;

    public M5RetentionAdmissionV1(
            ExactMetadataTransactionStoreV1 metadata,
            String stateKey,
            Sha256Digest protocolCellSha256,
            BindingIdentity binding) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.stateKey = requireText(stateKey, "stateKey", ExactMetadataTransactionStoreV1.MAX_KEY_BYTES);
        this.protocolCellSha256 = requireDigest(protocolCellSha256, "protocolCellSha256");
        this.binding = Objects.requireNonNull(binding, "binding");
    }

    public CompletionStage<Result> install(Caps caps) {
        State candidate = new State(protocolCellSha256, binding, 1, caps, List.of(), calculateUsage(List.of()));
        CanonicalBytes candidateBytes = encode(candidate);
        return metadata.compareAndSet(Optional.empty(), stateKey, candidateBytes)
                .thenCompose(outcome -> reconcile(Optional.empty(), candidate, candidateBytes, outcome, true));
    }

    public CompletionStage<Result> reserve(Reservation reservation) {
        Objects.requireNonNull(reservation, "reservation");
        return metadata.read(stateKey).thenCompose(current -> {
            VersionedValue exact = current.orElseThrow(
                    () -> new IllegalStateException("M5-C retention admission caps are not installed"));
            State state = decode(exact.canonicalStoredBytes());
            Optional<Reservation> existing = state.reservations().stream()
                    .filter(value -> value.reservationIdSha256().equals(reservation.reservationIdSha256()))
                    .findFirst();
            if (existing.isPresent()) {
                return CompletableFuture.completedFuture(
                        existing.orElseThrow().equals(reservation)
                                ? new Result(Outcome.EXISTING_EXACT, Optional.of(state), Optional.empty())
                                : new Result(Outcome.CONFLICT, Optional.empty(), Optional.empty()));
            }
            List<Reservation> reservations = new ArrayList<>(state.reservations());
            reservations.add(reservation);
            reservations.sort(
                    Comparator.comparing(value -> value.reservationIdSha256().toHex()));
            Usage usage = calculateUsage(reservations);
            Optional<LimitKindV1> exceeded = firstExceeded(state.caps(), usage);
            if (exceeded.isPresent()) {
                LimitKindV1 kind = exceeded.orElseThrow();
                AlertV1 alert = new AlertV1(
                        protocolCellSha256,
                        binding,
                        kind,
                        state.usage().value(kind),
                        reservation.reserved().get(kind),
                        state.caps().hard(kind),
                        reservation.oldestExactIdentitySha256(),
                        reservation.reserved().get(LimitKindV1.RETAINED_SOURCE_MAX_AGE_MILLIS),
                        reservation.blockingReferenceKinds(),
                        reservation.quarantineReason());
                return CompletableFuture.completedFuture(
                        new Result(Outcome.REJECTED_CAP, Optional.empty(), Optional.of(alert)));
            }
            State successor = new State(
                    protocolCellSha256,
                    binding,
                    Math.addExact(state.generation(), 1),
                    state.caps(),
                    reservations,
                    usage);
            CanonicalBytes candidateBytes = encode(successor);
            return metadata.compareAndSet(Optional.of(exact), stateKey, candidateBytes)
                    .thenCompose(outcome -> reconcile(Optional.of(exact), successor, candidateBytes, outcome, false));
        });
    }

    public CompletionStage<Optional<State>> read() {
        return metadata.read(stateKey).thenApply(value -> value.map(stored -> decode(stored.canonicalStoredBytes())));
    }

    private CompletionStage<Result> reconcile(
            Optional<VersionedValue> predecessor,
            State candidate,
            CanonicalBytes candidateBytes,
            MutationOutcome mutation,
            boolean install) {
        return metadata.read(stateKey)
                .thenApply(observed -> {
                    if (observed.isPresent()
                            && observed.orElseThrow().canonicalStoredBytes().equals(candidateBytes)) {
                        Outcome exact = mutation == MutationOutcome.APPLIED_EXACT
                                ? (install ? Outcome.INSTALLED : Outcome.APPLIED_EXACT)
                                : Outcome.EXISTING_EXACT;
                        return new Result(exact, Optional.of(candidate), Optional.empty());
                    }
                    if (observed.equals(predecessor)) {
                        return new Result(
                                mutation == MutationOutcome.DEFINITIVE_CONFLICT
                                        ? Outcome.CONFLICT
                                        : Outcome.DEFINITIVELY_NOT_APPLIED,
                                Optional.empty(),
                                Optional.empty());
                    }
                    return new Result(Outcome.CONFLICT, Optional.empty(), Optional.empty());
                })
                .exceptionally(ignored -> new Result(Outcome.RESPONSE_UNKNOWN, Optional.empty(), Optional.empty()));
    }

    public static CanonicalBytes encode(State state) {
        Objects.requireNonNull(state, "state");
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(MAGIC);
                output.writeInt(VERSION);
                writeDigest(output, state.protocolCellSha256());
                writeBinding(output, state.binding());
                output.writeLong(state.generation());
                writeValues(output, state.caps().hardLimits());
                output.writeInt(state.reservations().size());
                for (Reservation reservation : state.reservations()) {
                    writeDigest(output, reservation.reservationIdSha256());
                    writeDigest(output, reservation.oldestExactIdentitySha256());
                    writeValues(output, reservation.reserved());
                    output.writeInt(reservation.blockingReferenceKinds().size());
                    for (ReferenceKindV1 kind : reservation.blockingReferenceKinds()) {
                        output.writeByte(kind.ordinal() + 1);
                    }
                    output.writeBoolean(reservation.quarantineReason().isPresent());
                    if (reservation.quarantineReason().isPresent()) {
                        writeText(output, reservation.quarantineReason().orElseThrow());
                    }
                }
                writeValues(output, state.usage().values());
            }
            if (bytes.size() <= 0 || bytes.size() > MAX_STATE_BYTES) {
                throw new IllegalArgumentException("M5-C retention admission state exceeds its byte cap");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("in-memory retention admission encoding failed", failure);
        }
    }

    public static State decode(CanonicalBytes encoded) {
        if (encoded == null || encoded.isEmpty() || encoded.length() > MAX_STATE_BYTES) {
            throw new IllegalArgumentException("retention admission bytes are outside their hard cap");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded.toByteArray()))) {
            if (input.readInt() != MAGIC || input.readInt() != VERSION) {
                throw new IllegalArgumentException("retention admission magic/version differs");
            }
            Sha256Digest cell = readDigest(input);
            BindingIdentity binding = readBinding(input);
            long generation = input.readLong();
            Caps caps = new Caps(readValues(input));
            int reservationCount = input.readInt();
            if (reservationCount < 0 || reservationCount > MAX_RESERVATIONS) {
                throw new IllegalArgumentException("retention reservation count is outside its hard cap");
            }
            List<Reservation> reservations = new ArrayList<>(reservationCount);
            for (int index = 0; index < reservationCount; index++) {
                Sha256Digest id = readDigest(input);
                Sha256Digest oldest = readDigest(input);
                Map<LimitKindV1, Long> values = readValues(input);
                int blockingCount = input.readInt();
                if (blockingCount < 0 || blockingCount > ReferenceKindV1.values().length) {
                    throw new IllegalArgumentException("blocking reference count is outside its hard cap");
                }
                List<ReferenceKindV1> blocking = new ArrayList<>(blockingCount);
                for (int row = 0; row < blockingCount; row++) {
                    blocking.add(
                            enumValue(ReferenceKindV1.values(), input.readUnsignedByte(), "blocking reference kind"));
                }
                Optional<String> reason = input.readBoolean() ? Optional.of(readText(input)) : Optional.empty();
                reservations.add(new Reservation(id, oldest, values, blocking, reason));
            }
            State state = new State(cell, binding, generation, caps, reservations, new Usage(readValues(input)));
            if (input.read() != -1 || !encoded.equals(encode(state))) {
                throw new IllegalArgumentException("retention admission value is not canonical");
            }
            return state;
        } catch (EOFException failure) {
            throw new IllegalArgumentException("retention admission value is truncated", failure);
        } catch (IOException failure) {
            throw new IllegalArgumentException("retention admission value cannot be decoded", failure);
        }
    }

    public static Usage calculateUsage(List<Reservation> reservations) {
        EnumMap<LimitKindV1, Long> totals = zeroValues();
        for (Reservation reservation : reservations) {
            for (LimitKindV1 kind : LimitKindV1.values()) {
                long value = reservation.reserved().get(kind);
                totals.put(
                        kind,
                        kind == LimitKindV1.RETAINED_SOURCE_MAX_AGE_MILLIS
                                ? Math.max(totals.get(kind), value)
                                : Math.addExact(totals.get(kind), value));
            }
        }
        return new Usage(totals);
    }

    private static Optional<LimitKindV1> firstExceeded(Caps caps, Usage usage) {
        for (LimitKindV1 kind : LimitKindV1.values()) {
            if (usage.value(kind) > caps.hard(kind)) {
                return Optional.of(kind);
            }
        }
        return Optional.empty();
    }

    private static Map<LimitKindV1, Long> exactMap(Map<LimitKindV1, Long> values, boolean positive, String label) {
        Objects.requireNonNull(values, label);
        if (!values.keySet().equals(java.util.EnumSet.allOf(LimitKindV1.class))) {
            throw new IllegalArgumentException(label + " do not cover the closed limit inventory");
        }
        EnumMap<LimitKindV1, Long> copy = new EnumMap<>(LimitKindV1.class);
        for (LimitKindV1 kind : LimitKindV1.values()) {
            Long value = Objects.requireNonNull(values.get(kind), label + " value");
            if ((positive && value <= 0) || (!positive && value < 0)) {
                throw new IllegalArgumentException(label + " contain an invalid value for " + kind);
            }
            copy.put(kind, value);
        }
        return Map.copyOf(copy);
    }

    private static EnumMap<LimitKindV1, Long> zeroValues() {
        EnumMap<LimitKindV1, Long> values = new EnumMap<>(LimitKindV1.class);
        for (LimitKindV1 kind : LimitKindV1.values()) {
            values.put(kind, 0L);
        }
        return values;
    }

    private static void writeValues(DataOutputStream output, Map<LimitKindV1, Long> values) throws IOException {
        for (LimitKindV1 kind : LimitKindV1.values()) {
            output.writeLong(values.get(kind));
        }
    }

    private static Map<LimitKindV1, Long> readValues(DataInputStream input) throws IOException {
        EnumMap<LimitKindV1, Long> values = new EnumMap<>(LimitKindV1.class);
        for (LimitKindV1 kind : LimitKindV1.values()) {
            values.put(kind, input.readLong());
        }
        return values;
    }

    private static void writeBinding(DataOutputStream output, BindingIdentity binding) throws IOException {
        writeDigest(output, binding.bindingId().digest());
        writeDigest(output, binding.incarnationSha256());
        writeDigest(output, binding.storageEpochSha256());
    }

    private static BindingIdentity readBinding(DataInputStream input) throws IOException {
        return new BindingIdentity(new TopicBindingId(readDigest(input)), readDigest(input), readDigest(input));
    }

    private static void writeDigest(DataOutputStream output, Sha256Digest digest) throws IOException {
        output.write(requireDigest(digest, "digest").bytes().toByteArray());
    }

    private static Sha256Digest readDigest(DataInputStream input) throws IOException {
        byte[] bytes = input.readNBytes(Sha256Digest.LENGTH);
        if (bytes.length != Sha256Digest.LENGTH) {
            throw new EOFException("truncated SHA-256 digest");
        }
        return requireDigest(Sha256Digest.copyOf(bytes), "digest");
    }

    private static void writeText(DataOutputStream output, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0 || bytes.length > MAX_ALERT_TEXT_BYTES) {
            throw new IllegalArgumentException("alert text is outside its byte cap");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readText(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > MAX_ALERT_TEXT_BYTES) {
            throw new IllegalArgumentException("alert text is outside its byte cap");
        }
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) {
            throw new EOFException("truncated alert text");
        }
        return CanonicalUtf8.fromBytes(bytes).value();
    }

    private static <T extends Enum<T>> T enumValue(T[] values, int encoded, String label) {
        if (encoded <= 0 || encoded > values.length) {
            throw new IllegalArgumentException(label + " code is unknown");
        }
        return values[encoded - 1];
    }

    private static Sha256Digest requireDigest(Sha256Digest digest, String name) {
        M5RetentionRecordsV1.requireDigest(digest, name);
        return digest;
    }

    private static String requireText(String value, String name, int maximumBytes) {
        Objects.requireNonNull(value, name);
        int length = value.getBytes(StandardCharsets.UTF_8).length;
        if (value.isBlank() || length > maximumBytes) {
            throw new IllegalArgumentException(name + " is blank or exceeds its byte cap");
        }
        return value;
    }
}

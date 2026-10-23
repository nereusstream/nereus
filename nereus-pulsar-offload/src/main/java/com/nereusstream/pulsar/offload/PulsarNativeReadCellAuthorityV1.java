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

package com.nereusstream.pulsar.offload;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.ProtocolCellIdentityCodecV1;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import io.oxia.client.api.AsyncOxiaClient;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** One exact durable Cell allocator owner. A lost active owner cannot be replaced by a new local budget. */
final class PulsarNativeReadCellAuthorityV1 {
    private static final int MAGIC = 0x4e504348;
    private static final int HEAD_BYTES = 144;
    private static final int HOLD_BYTES = 144;
    private static final int MAX_HOLDS = 1024;
    private final PulsarNativeBindingV1 binding;
    private final Oxia09ExactMetadataTransactionStoreV1 store;
    private final Sha256Digest domain;
    private final CanonicalBytes profile;
    private final Sha256Digest profileSha;
    private final UUID owner = UUID.randomUUID();
    private final String profileKey;
    private final String headKey;
    private CompletableFuture<Void> admission;
    private long generation;
    private CanonicalBytes active;
    private boolean headObserved;
    private CompletableFuture<Void> mutations = CompletableFuture.completedFuture(null);

    PulsarNativeReadCellAuthorityV1(AsyncOxiaClient client, PulsarNativeBindingV1 binding, CanonicalBytes profile) {
        this.binding = binding;
        this.profile = profile;
        store = new Oxia09ExactMetadataTransactionStoreV1(client);
        var cell = ProtocolCellIdentityCodecV1.encode(
                binding.authority().aggregate().binding().cellIdentity());
        var namespace = binding.metadataNamespace().encode();
        domain = Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(12 + namespace.length() + cell.length())
                .putInt(0x4e504344)
                .putInt(namespace.length())
                .put(namespace.toByteArray())
                .putInt(cell.length())
                .put(cell.toByteArray())
                .array()));
        profileSha = Sha256Digest.hash(profile);
        String root = "/nereus/cells/native-read_" + domain.toHex();
        profileKey = root + "/profile";
        headKey = root + "/owner";
    }

    String headKey() {
        return headKey;
    }

    synchronized CanonicalBytes nativeOwnerIdentity() {
        if (generation < 1 || admission == null || !admission.isDone() || admission.isCompletedExceptionally()) {
            throw new IllegalStateException("Native Cell owner was not admitted");
        }
        return CanonicalBytes.copyOf(ByteBuffer.allocate(96)
                .putInt(0x4e50434f)
                .putInt(1)
                .put(domain.bytes().toByteArray())
                .put(profileSha.bytes().toByteArray())
                .putLong(generation)
                .putLong(owner.getMostSignificantBits())
                .putLong(owner.getLeastSignificantBits())
                .array());
    }

    CompletableFuture<Void> requireNativeOwner(CanonicalBytes expected) {
        return store.read(headKey)
                .thenApply(found -> {
                    var head = readHead(found.orElseThrow().canonicalStoredBytes());
                    requireActiveOwner(head);
                    if (!expected.equals(nativeOwnerIdentity())) {
                        throw new IllegalStateException("Native Cell allocator owner differs");
                    }
                    return (Void) null;
                })
                .toCompletableFuture();
    }

    record OwnerEpoch(
            long generation,
            com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1 scope,
            CanonicalBytes nativeOwner) {}

    /**
     * The immutable admitted Cell profile scopes the non-reused generation stored in the actual owner head.
     * Reservation revisions do not change ownership. Every read still decodes that head and verifies this
     * original private owner; profile existence alone never proves ownership.
     */
    CompletableFuture<OwnerEpoch> captureOwnerEpoch() {
        return store.read(profileKey)
                .thenCompose(found -> {
                    var exactProfile = found.orElseThrow();
                    requireProfile(exactProfile.canonicalStoredBytes());
                    return store.read(headKey).thenApply(actual -> {
                        var head = readHead(actual.orElseThrow().canonicalStoredBytes());
                        requireActiveOwner(head);
                        return new OwnerEpoch(
                                head.generation(),
                                new com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1(
                                        exactProfile.key(),
                                        exactProfile.metadataVersion(),
                                        exactProfile.canonicalStoredSha256()),
                                nativeOwnerIdentity());
                    });
                })
                .toCompletableFuture();
    }

    record Reservations(
            java.util.Map<String, VersionedValue> facts,
            java.util.Map<UUID, PulsarNativeReadCellBudgetV1.Usage> otherReservations) {
        Reservations {
            facts = java.util.Map.copyOf(facts);
            otherReservations = java.util.Map.copyOf(otherReservations);
        }
    }

    /** Read only: the original observation's reservation must still be present under this admitted owner. */
    CompletableFuture<Reservations> captureReservations(
            UUID operation, BindingIdentity observedBinding, PulsarNativeReadCellBudgetV1.Usage charge) {
        return store.read(profileKey)
                .thenCompose(profileValue -> {
                    var exactProfile =
                            profileValue.orElseThrow(() -> new IllegalStateException("Native Cell profile is absent"));
                    requireProfile(exactProfile.canonicalStoredBytes());
                    return store.read(headKey).thenCompose(headValue -> {
                        var exactHead =
                                headValue.orElseThrow(() -> new IllegalStateException("Native Cell head is absent"));
                        var head = readHead(exactHead.canonicalStoredBytes());
                        requireActiveOwner(head);
                        var original = new Hold(operation, observedBinding, charge);
                        if (!head.holds().contains(original)) {
                            throw new IllegalStateException(
                                    "Native root observation lost its original Cell reservation");
                        }
                        var others = new java.util.TreeMap<UUID, PulsarNativeReadCellBudgetV1.Usage>();
                        for (var hold : head.holds()) {
                            if (hold.binding().equals(observedBinding)
                                    && !hold.operation().equals(operation)) {
                                others.put(hold.operation(), hold.charge());
                            }
                        }
                        var facts = java.util.Map.of(profileKey, exactProfile, headKey, exactHead);
                        CompletableFuture<Void> fresh = CompletableFuture.completedFuture(null);
                        for (var fact : facts.values()) {
                            fresh = fresh.thenCompose(ignored -> store.read(fact.key())
                                    .thenAccept(current -> {
                                        if (current.filter(fact::equals).isEmpty()) {
                                            throw new IllegalStateException(
                                                    "Native Cell reservation observation changed");
                                        }
                                    })
                                    .toCompletableFuture());
                        }
                        return fresh.thenApply(ignored -> new Reservations(facts, others));
                    });
                })
                .toCompletableFuture();
    }

    boolean isDrainedValue(CanonicalBytes bytes) {
        return readHead(bytes).drained();
    }

    boolean hasReservationValue(CanonicalBytes bytes) {
        return !readHead(bytes).holds().isEmpty();
    }

    synchronized CompletableFuture<Void> admit() {
        if (admission != null && (!admission.isDone() || !admission.isCompletedExceptionally())) {
            return admission.copy();
        }
        admission = binding.revalidate()
                .thenCompose(ignored -> store.read(profileKey))
                .thenCompose(found -> {
                    var initial = initialProfile();
                    if (found.isEmpty()) {
                        return store.compareAndSet(Optional.empty(), profileKey, initial)
                                .thenCompose(result -> store.read(profileKey));
                    }
                    return CompletableFuture.completedFuture(found);
                })
                .thenCompose(found -> {
                    var stored = found.orElseThrow(
                            () -> new IllegalStateException("Native Cell profile publication is unknown"));
                    var creator = requireProfile(stored.canonicalStoredBytes());
                    return store.read(headKey).thenCompose(head -> {
                        if (head.isEmpty()) {
                            if (headObserved || !creator.equals(owner)) {
                                throw new IllegalStateException(
                                        "Native Cell owner head is absent; no automatic recreation");
                            }
                            generation = 1;
                            active = headBytes(new Head(generation, owner, false, 0, List.of()));
                        } else {
                            headObserved = true;
                            var current = readHead(head.orElseThrow().canonicalStoredBytes());
                            if (current.owner().equals(owner) && !current.drained()) {
                                generation = current.generation();
                                active = head.orElseThrow().canonicalStoredBytes();
                                return CompletableFuture.completedFuture(null);
                            }
                            if (!current.drained()) {
                                throw new IllegalStateException("Native Cell allocator has no confirmed durable drain");
                            }
                            generation = Math.addExact(current.generation(), 1);
                            active = headBytes(new Head(generation, owner, false, 0, List.of()));
                        }
                        return store.compareAndSet(head, headKey, active)
                                .thenCompose(result -> store.read(headKey))
                                .thenApply(observed -> {
                                    if (observed.filter(value ->
                                                    value.canonicalStoredBytes().equals(active))
                                            .isEmpty()) {
                                        throw new IllegalStateException(
                                                "Native Cell owner grant is unknown or conflicts");
                                    }
                                    headObserved = true;
                                    return (Void) null;
                                });
                    });
                })
                .thenCompose(ignored -> binding.revalidate())
                .toCompletableFuture();
        return admission.copy();
    }

    CompletableFuture<Void> reserve(
            UUID operation, BindingIdentity binding, PulsarNativeReadCellBudgetV1.Usage charge) {
        return mutate(() -> updateHold(new Hold(operation, binding, charge), false));
    }

    CompletableFuture<Void> release(
            UUID operation, BindingIdentity binding, PulsarNativeReadCellBudgetV1.Usage charge) {
        return mutate(() -> updateHold(new Hold(operation, binding, charge), true));
    }

    private synchronized CompletableFuture<Void> mutate(Supplier<CompletableFuture<Void>> operation) {
        var next = mutations.handle((ignored, failure) -> null).thenCompose(ignored -> operation.get());
        mutations = next.handle((ignored, failure) -> (Void) null);
        return next.copy();
    }

    private CompletableFuture<Void> updateHold(Hold hold, boolean release) {
        return store.read(headKey)
                .thenCompose(found -> {
                    var exact = found.orElseThrow(
                            () -> new IllegalStateException("Native Cell reservation head is absent"));
                    var current = readHead(exact.canonicalStoredBytes());
                    requireActiveOwner(current);
                    var existing = current.holds().stream()
                            .filter(value -> value.operation().equals(hold.operation()))
                            .findFirst();
                    if (existing.isPresent() && !existing.orElseThrow().equals(hold)) {
                        throw new IllegalStateException("Native Cell reservation operation facts differ");
                    }
                    if (!release && existing.isPresent()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    var holds = new ArrayList<>(current.holds());
                    if (release) {
                        existing.ifPresent(holds::remove);
                    } else {
                        if (holds.size() == MAX_HOLDS) {
                            throw new IllegalStateException("Native Cell durable reservation inventory exhausted");
                        }
                        holds.add(hold);
                    }
                    holds.sort(Comparator.comparing(value -> value.operation().toString()));
                    var next = new Head(
                            generation, owner, false, Math.addExact(current.revision(), 1), List.copyOf(holds));
                    var candidate = headBytes(next);
                    return store.compareAndSet(Optional.of(exact), headKey, candidate)
                            .thenCompose(result -> store.read(headKey))
                            .thenApply(observed -> {
                                if (observed.filter(value ->
                                                value.canonicalStoredBytes().equals(candidate))
                                        .isEmpty()) {
                                    throw new IllegalStateException("Native Cell reservation mutation remains unknown");
                                }
                                return (Void) null;
                            });
                })
                .toCompletableFuture();
    }

    private void requireActiveOwner(Head head) {
        if (head.drained() || head.generation() != generation || !head.owner().equals(owner)) {
            throw new IllegalStateException("Native Cell reservation belongs to another allocator owner");
        }
    }

    CompletableFuture<Void> closeAfterLocalDrain() {
        if (active == null || admission == null || !admission.isDone() || admission.isCompletedExceptionally()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Native Cell allocator was not admitted"));
        }
        return mutate(() -> store.read(headKey)
                .thenCompose(found -> {
                    var exact = found.orElseThrow(() -> new IllegalStateException("Native Cell owner head is absent"));
                    var head = readHead(exact.canonicalStoredBytes());
                    if (head.owner().equals(owner) && head.generation() == generation && head.drained()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    requireActiveOwner(head);
                    if (!head.holds().isEmpty()) {
                        throw new IllegalStateException("Native Cell durable reservation inventory is not drained");
                    }
                    var terminal =
                            headBytes(new Head(generation, owner, true, Math.addExact(head.revision(), 1), List.of()));
                    return store.compareAndSet(Optional.of(exact), headKey, terminal)
                            .thenCompose(result -> store.read(headKey))
                            .thenApply(current -> {
                                if (current.filter(value ->
                                                value.canonicalStoredBytes().equals(terminal))
                                        .isEmpty()) {
                                    throw new IllegalStateException("Native Cell durable drain remains unknown");
                                }
                                return (Void) null;
                            });
                })
                .toCompletableFuture());
    }

    CompletableFuture<PulsarNativeReadCellBudgetV1.Usage> durableUsage() {
        return store.read(headKey)
                .thenApply(found -> {
                    var head = readHead(found.orElseThrow().canonicalStoredBytes());
                    var total = new PulsarNativeReadCellBudgetV1.Usage(0, 0, 0, 0);
                    for (var hold : head.holds()) {
                        total = new PulsarNativeReadCellBudgetV1.Usage(
                                Math.addExact(total.owners(), hold.charge().owners()),
                                Math.addExact(total.readSlots(), hold.charge().readSlots()),
                                Math.addExact(
                                        total.encodedBytes(), hold.charge().encodedBytes()),
                                Math.addExact(
                                        total.decodedBytes(), hold.charge().decodedBytes()));
                    }
                    return total;
                })
                .toCompletableFuture()
                .copy();
    }

    private CanonicalBytes initialProfile() {
        return CanonicalBytes.copyOf(ByteBuffer.allocate(24 + profile.length())
                .putInt(0x4e504350)
                .putInt(1)
                .putLong(owner.getMostSignificantBits())
                .putLong(owner.getLeastSignificantBits())
                .put(profile.toByteArray())
                .array());
    }

    private UUID requireProfile(CanonicalBytes stored) {
        var in = ByteBuffer.wrap(stored.toByteArray());
        if (in.remaining() != 24 + profile.length() || in.getInt() != 0x4e504350 || in.getInt() != 1) {
            throw new IllegalStateException("Native Cell profile is invalid");
        }
        var creator = new UUID(in.getLong(), in.getLong());
        byte[] body = new byte[in.remaining()];
        in.get(body);
        if (!CanonicalBytes.copyOf(body).equals(profile)) {
            throw new IllegalStateException("Native Cell fixed profile differs");
        }
        return creator;
    }

    private record Hold(UUID operation, BindingIdentity binding, PulsarNativeReadCellBudgetV1.Usage charge) {}

    private record Head(long generation, UUID owner, boolean drained, long revision, List<Hold> holds) {}

    private Head readHead(CanonicalBytes bytes) {
        var in = ByteBuffer.wrap(bytes.toByteArray());
        if (in.remaining() < 132 || in.getInt() != MAGIC) {
            throw new IllegalStateException("Native Cell owner head is invalid");
        }
        int version = in.getInt();
        if (version != 1 && version != 2) {
            throw new IllegalStateException("Native Cell owner head version is unsupported");
        }
        byte[] actualDomain = new byte[32];
        byte[] actualProfile = new byte[32];
        in.get(actualDomain).get(actualProfile);
        long epoch = in.getLong();
        var identity = new UUID(in.getLong(), in.getLong());
        int state = in.getInt();
        long revision = version == 1 ? 0 : in.getLong();
        int count = version == 1 ? 0 : in.getInt();
        int headerBytes = version == 1 ? 132 : HEAD_BYTES;
        if (count < 0 || count > MAX_HOLDS || bytes.length() != headerBytes + HOLD_BYTES * count) {
            throw new IllegalStateException("Native Cell reservation count/bytes differ");
        }
        var holds = new ArrayList<Hold>();
        for (int i = 0; i < count; i++) {
            var operation = new UUID(in.getLong(), in.getLong());
            byte[] bindingId = new byte[32];
            byte[] incarnation = new byte[32];
            byte[] storageEpoch = new byte[32];
            in.get(bindingId).get(incarnation).get(storageEpoch);
            var binding = new BindingIdentity(
                    new TopicBindingId(Sha256Digest.copyOf(bindingId)),
                    Sha256Digest.copyOf(incarnation),
                    Sha256Digest.copyOf(storageEpoch));
            var charge = new PulsarNativeReadCellBudgetV1.Usage(in.getLong(), in.getLong(), in.getLong(), in.getLong());
            holds.add(new Hold(operation, binding, charge));
        }
        byte[] digest = new byte[32];
        in.get(digest);
        if (!Sha256Digest.copyOf(actualDomain).equals(domain)
                || !Sha256Digest.copyOf(actualProfile).equals(profileSha)
                || epoch < 1
                || revision < 0
                || state < 0
                || state > 1
                || identity.equals(new UUID(0, 0))
                || state == 1 && !holds.isEmpty()
                || holds.stream().map(Hold::operation).distinct().count() != holds.size()
                || !holds.equals(holds.stream()
                        .sorted(Comparator.comparing(value -> value.operation().toString()))
                        .toList())
                || !Sha256Digest.hash(CanonicalBytes.copyOf(
                                java.util.Arrays.copyOf(bytes.toByteArray(), bytes.length() - 32)))
                        .equals(Sha256Digest.copyOf(digest))) {
            throw new IllegalStateException("Native Cell owner head identity/checksum differs");
        }
        return new Head(epoch, identity, state == 1, revision, List.copyOf(holds));
    }

    private CanonicalBytes headBytes(Head head) {
        var out = ByteBuffer.allocate(HEAD_BYTES + HOLD_BYTES * head.holds().size())
                .putInt(MAGIC)
                .putInt(2)
                .put(domain.bytes().toByteArray())
                .put(profileSha.bytes().toByteArray())
                .putLong(head.generation())
                .putLong(head.owner().getMostSignificantBits())
                .putLong(head.owner().getLeastSignificantBits())
                .putInt(head.drained() ? 1 : 0)
                .putLong(head.revision())
                .putInt(head.holds().size());
        for (var hold : head.holds()) {
            out.putLong(hold.operation().getMostSignificantBits())
                    .putLong(hold.operation().getLeastSignificantBits())
                    .put(hold.binding().bindingId().digest().bytes().toByteArray())
                    .put(hold.binding().incarnationSha256().bytes().toByteArray())
                    .put(hold.binding().storageEpochSha256().bytes().toByteArray())
                    .putLong(hold.charge().owners())
                    .putLong(hold.charge().readSlots())
                    .putLong(hold.charge().encodedBytes())
                    .putLong(hold.charge().decodedBytes());
        }
        out.put(Sha256Digest.hash(CanonicalBytes.copyOf(java.util.Arrays.copyOf(out.array(), out.capacity() - 32)))
                .bytes()
                .toByteArray());
        return CanonicalBytes.copyOf(out.array());
    }
}

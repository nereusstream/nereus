/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.metadata.oxia.v2.retention;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.MetadataVersionMapper;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Completion;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Context;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

/**
 * Bounded, append-only member observations on the native physical namespace route. Registration precedes a native
 * resource's first creation/reference and uses its existing physical ticket. A scan is complete only after every
 * applicable reference-admitting writer is enrolled; this record alone is not deletion qualification.
 */
public final class OxiaPhysicalMemberInventoryV2 {
    public static final String PREFIX = M5GcQuotaRecordsV2.PHYSICAL_MEMBER_PREFIX;
    private static final int MAGIC = 0x4d35504d;
    private static final int MEMBER_BYTES = 132;
    private static final int FIXED_BYTES = 106;
    private static final Comparator<Member> ORDER = Comparator.comparing(Member::sortKey);

    /** Stable protocol membership; Owner/worker epochs and proof generations do not create another member. */
    public record Member(PositionDomain domain, Sha256Digest providerScope, BindingIdentity binding) {
        public Member {
            Objects.requireNonNull(domain, "domain");
            requireDigest(providerScope, "providerScope");
            Objects.requireNonNull(binding, "binding");
        }

        private String sortKey() {
            return domain.ordinal() + "/" + providerScope.toHex() + "/"
                    + binding.bindingId().digest().toHex() + "/"
                    + binding.incarnationSha256().toHex() + "/"
                    + binding.storageEpochSha256().toHex();
        }

        private void write(ByteBuffer out) {
            out.putInt(domain.ordinal())
                    .put(providerScope.bytes().toByteArray())
                    .put(binding.bindingId().digest().bytes().toByteArray())
                    .put(binding.incarnationSha256().bytes().toByteArray())
                    .put(binding.storageEpochSha256().bytes().toByteArray());
        }
    }

    /** Historical members remain until separately qualified metadata retirement; a rescan cannot forget them. */
    public record Inventory(Sha256Digest resource, Sha256Digest protocolCell, List<Member> members) {
        public Inventory {
            requireDigest(resource, "resource");
            requireDigest(protocolCell, "protocolCell");
            members = List.copyOf(members);
            if (members.isEmpty()
                    || members.size() > DeleteEligibilitySnapshotV2.MAX_MEMBERS
                    || FIXED_BYTES + MEMBER_BYTES * members.size() > M5GcQuotaRecordsV2.PHYSICAL_MEMBER_MAX_VALUE_BYTES
                    || !members.equals(members.stream().sorted(ORDER).distinct().toList())) {
                throw new IllegalArgumentException("physical members must be bounded, sorted and unique");
            }
        }

        public CanonicalBytes encode() {
            var out = ByteBuffer.allocate(FIXED_BYTES + MEMBER_BYTES * members.size())
                    .putInt(MAGIC)
                    .putShort((short) 2)
                    .put(resource.bytes().toByteArray())
                    .put(protocolCell.bytes().toByteArray())
                    .putInt(members.size());
            members.forEach(member -> member.write(out));
            out.put(Sha256Digest.hash(CanonicalBytes.copyOf(Arrays.copyOf(out.array(), out.position())))
                    .bytes()
                    .toByteArray());
            return CanonicalBytes.copyOf(out.array());
        }

        public static Inventory decode(CanonicalBytes bytes) {
            if (bytes.length() < FIXED_BYTES + MEMBER_BYTES
                    || bytes.length() > M5GcQuotaRecordsV2.PHYSICAL_MEMBER_MAX_VALUE_BYTES) {
                throw new IllegalArgumentException("physical member inventory has an invalid length");
            }
            var raw = bytes.toByteArray();
            var checksum = Sha256Digest.hash(CanonicalBytes.copyOf(Arrays.copyOf(raw, raw.length - 32)));
            if (!checksum.equals(Sha256Digest.copyOf(Arrays.copyOfRange(raw, raw.length - 32, raw.length)))) {
                throw new IllegalArgumentException("physical member inventory checksum differs");
            }
            var in = ByteBuffer.wrap(raw);
            if (in.getInt() != MAGIC || in.getShort() != 2) {
                throw new IllegalArgumentException("physical member inventory wire differs");
            }
            var resource = digest(in);
            var cell = digest(in);
            int count = in.getInt();
            if (count < 1
                    || count > DeleteEligibilitySnapshotV2.MAX_MEMBERS
                    || raw.length != FIXED_BYTES + MEMBER_BYTES * count) {
                throw new IllegalArgumentException("physical member inventory count differs");
            }
            var members = new ArrayList<Member>();
            for (int index = 0; index < count; index++) {
                int domain = in.getInt();
                if (domain < 0 || domain >= PositionDomain.values().length) {
                    throw new IllegalArgumentException("physical member domain differs");
                }
                members.add(new Member(
                        PositionDomain.values()[domain],
                        digest(in),
                        new BindingIdentity(new TopicBindingId(digest(in)), digest(in), digest(in))));
            }
            return new Inventory(resource, cell, members);
        }
    }

    public record Fact(Inventory inventory, AuthorityFactV1 authority, CanonicalBytes canonicalStoredBytes) {
        public Fact {
            Objects.requireNonNull(inventory, "inventory");
            Objects.requireNonNull(authority, "authority");
            if (!inventory.encode().equals(canonicalStoredBytes)
                    || !authority.valueSha256().equals(Sha256Digest.hash(canonicalStoredBytes))) {
                throw new IllegalArgumentException("physical member fact differs from its native bytes");
            }
        }
    }

    /** One observed hash slot; absent authority/member/reservation values remain unresolved, never invented. */
    public record ResourceObservation(
            Sha256Digest resourceSha256,
            Optional<com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2> storedAuthority,
            Optional<M5GcQuotaRecordsV2.Entry> reservation,
            Optional<Fact> members) {
        public ResourceObservation {
            requireDigest(resourceSha256, "resourceSha256");
            Objects.requireNonNull(storedAuthority, "storedAuthority");
            Objects.requireNonNull(reservation, "reservation");
            Objects.requireNonNull(members, "members");
            if (storedAuthority
                            .filter(value -> !value.resource().sha256().equals(resourceSha256))
                            .isPresent()
                    || reservation
                            .filter(value -> !value.resource().equals(resourceSha256))
                            .isPresent()
                    || members.filter(value -> !value.inventory().resource().equals(resourceSha256))
                            .isPresent()) {
                throw new IllegalArgumentException("physical namespace resource rows disagree");
            }
        }
    }

    /** Observed metadata only. Caller owns scan admission and must still fence every applicable physical writer. */
    public static final class NamespaceSnapshot {
        private final com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2 binding;
        private final java.util.Map<
                        String, com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue>
                facts;
        private final List<ResourceObservation> resources;
        private final List<com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3> births;

        private NamespaceSnapshot(
                com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2 binding,
                java.util.Map<
                                String,
                                com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue>
                        facts,
                List<ResourceObservation> resources,
                List<com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3> births) {
            this.binding = binding;
            this.facts = java.util.Map.copyOf(facts);
            this.resources = List.copyOf(resources);
            this.births = List.copyOf(births);
        }

        public com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2 binding() {
            return binding;
        }

        public java.util.Map<
                        String, com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue>
                facts() {
            return facts;
        }

        public List<ResourceObservation> resources() {
            return resources;
        }

        public List<com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3> births() {
            return births;
        }
    }

    /** Bounded namespace metadata, including DONE history and incomplete birth/registration slots. */
    public CompletionStage<NamespaceSnapshot> captureNamespace(int maximumRows, long maximumBytes) {
        if (maximumRows < 1
                || maximumRows > com.nereusstream.storage.object.retention.M5RetentionRecordsV1.MAX_REFERENCE_ROWS
                || maximumBytes < 1
                || maximumBytes > com.nereusstream.storage.object.retention.M5RetentionRecordsV1.MAX_SCAN_BYTES) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("physical namespace observation bounds are invalid"));
        }
        var layout = route.memberScanLayout();
        return route.requireMemberScanRoute()
                .thenCompose(binding -> readHead(layout).thenCompose(head -> {
                    if (head.canonicalStoredBytes().length() > maximumBytes) {
                        return CompletableFuture.failedFuture(new IllegalArgumentException(
                                "physical namespace quota head exceeds its admitted bound"));
                    }
                    return route.scanMemberNamespace(
                                    maximumRows - 1,
                                    maximumBytes - head.canonicalStoredBytes().length())
                            .thenCompose(captured -> {
                                var facts = new java.util.TreeMap<>(captured);
                                facts.put(head.key(), head);
                                var snapshot = decodeNamespace(binding, layout, facts);
                                CompletionStage<Void> fresh = CompletableFuture.completedFuture(null);
                                for (var fact : facts.values()) {
                                    fresh = fresh.thenCompose(
                                            ignored -> client.read(fact.key()).thenAccept(current -> {
                                                if (current.isEmpty()
                                                        || !toValue(current.orElseThrow())
                                                                .equals(fact)) {
                                                    throw new IllegalStateException(
                                                            "physical namespace record changed");
                                                }
                                            }));
                                }
                                return fresh.thenCompose(ignored -> route.scanMemberNamespace(
                                                maximumRows - 1,
                                                maximumBytes
                                                        - head.canonicalStoredBytes()
                                                                .length()))
                                        .thenCompose(current -> {
                                            if (!current.equals(captured)) {
                                                return CompletableFuture.failedFuture(new IllegalStateException(
                                                        "physical namespace key inventory changed during capture"));
                                            }
                                            return readHead(layout).thenCompose(now -> {
                                                if (!now.equals(head)) {
                                                    return CompletableFuture.failedFuture(new IllegalStateException(
                                                            "physical namespace quota head changed during capture"));
                                                }
                                                return route.requireMemberScanRoute()
                                                        .thenApply(after -> {
                                                            if (!after.equals(binding)) {
                                                                throw new IllegalStateException(
                                                                        "physical namespace route changed");
                                                            }
                                                            return snapshot;
                                                        });
                                            });
                                        });
                            });
                }));
    }

    private CompletionStage<com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue>
            readHead(M5GcQuotaRecordsV2.Layout layout) {
        return client.read(layout.nativeKey(M5GcQuotaRecordsV2.HEAD_KEY)).thenApply(value -> {
            var head = toValue(
                    value.orElseThrow(() -> new IllegalStateException("physical namespace quota head is missing")));
            layout.verify(M5GcQuotaRecordsV2.Head.decode(head.canonicalStoredBytes()));
            return head;
        });
    }

    private NamespaceSnapshot decodeNamespace(
            com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2 binding,
            M5GcQuotaRecordsV2.Layout layout,
            java.util.Map<
                            String,
                            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue>
                    facts) {
        var authorities =
                new java.util.TreeMap<String, com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2>();
        var reservations = new java.util.TreeMap<String, M5GcQuotaRecordsV2.Entry>();
        var inventories = new java.util.TreeMap<String, Fact>();
        var births = new ArrayList<com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3>();
        for (var fact : facts.values()) {
            String key = fact.key().substring(root.length() + 1);
            if (!layout.nativeKey(key).equals(fact.key())) {
                throw new IllegalArgumentException("physical namespace key differs from its route");
            }
            if (key.equals(M5GcQuotaRecordsV2.HEAD_KEY)) {
                continue;
            }
            if (key.startsWith("v2/physical-delete-m5-v2/")) {
                var projected =
                        com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue.of(
                                key, fact.canonicalStoredBytes(), fact.metadataVersion());
                var stored = com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2.decode(projected);
                layout.requireResource(stored.resource());
                authorities.put(stored.resource().sha256().toHex(), stored);
            } else if (key.startsWith(M5GcQuotaRecordsV2.ENTRY_PREFIX)) {
                var entry = M5GcQuotaRecordsV2.Entry.decode(fact.canonicalStoredBytes());
                layout.verify(entry);
                if (!entry.key().equals(key)) {
                    throw new IllegalArgumentException("physical reservation key differs");
                }
                reservations.put(entry.resource().toHex(), entry);
            } else if (key.startsWith(PREFIX)) {
                var inventory = Inventory.decode(fact.canonicalStoredBytes());
                if (!M5GcQuotaRecordsV2.physicalMemberKey(inventory.resource()).equals(key)) {
                    throw new IllegalArgumentException("physical inventory key differs from its resource");
                }
                inventories.put(
                        inventory.resource().toHex(),
                        new Fact(
                                inventory,
                                new AuthorityFactV1(fact.key(), fact.metadataVersion(), fact.canonicalStoredSha256()),
                                fact.canonicalStoredBytes()));
            } else if (key.startsWith(com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3.PREFIX)) {
                var birth =
                        com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3.decode(fact.canonicalStoredBytes());
                if (!birth.scope().equals(layout.scope()) || !birth.key().equals(key)) {
                    throw new IllegalArgumentException("physical birth key/scope differs");
                }
                birth.resource().ifPresent(layout::requireResource);
                births.add(birth);
            } else {
                throw new IllegalArgumentException("unknown physical namespace record kind");
            }
        }
        var ids = new java.util.TreeSet<String>();
        ids.addAll(authorities.keySet());
        ids.addAll(reservations.keySet());
        ids.addAll(inventories.keySet());
        var resources = ids.stream()
                .map(id -> new ResourceObservation(
                        Sha256Digest.copyOf(java.util.HexFormat.of().parseHex(id)),
                                Optional.ofNullable(authorities.get(id)),
                        Optional.ofNullable(reservations.get(id)), Optional.ofNullable(inventories.get(id))))
                .toList();
        return new NamespaceSnapshot(binding, facts, resources, births);
    }

    private static com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue toValue(
            AuthorityRecord value) {
        return com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue.of(
                value.key(), value.storedBytes(), MetadataVersionMapper.fromOxia(value.versionId()));
    }

    private record Registration(Fact fact, Throwable failure) {}

    private final OxiaConditionalClient client;
    private final OxiaQuotaTargetDeleteStoreV2 route;
    private final M5TargetDeleteMultiWriterGuardV2 guard;
    private final String root;

    OxiaPhysicalMemberInventoryV2(
            OxiaConditionalClient client,
            OxiaQuotaTargetDeleteStoreV2 route,
            M5TargetDeleteMultiWriterGuardV2 guard,
            String root) {
        this.client = Objects.requireNonNull(client, "client");
        this.route = Objects.requireNonNull(route, "route");
        this.guard = Objects.requireNonNull(guard, "guard");
        this.root = Objects.requireNonNull(root, "root");
    }

    public String nativeKey(PhysicalResourceIdV2 resource) {
        return root + "/" + PREFIX + resource.sha256().toHex() + "/inventory-v2";
    }

    /** Missing registration is unresolved; reads never create, adopt or replace an absent inventory. */
    public CompletionStage<Fact> readFact(PhysicalResourceIdV2 resource) {
        return route.requireActiveResource(resource)
                .thenCompose(binding -> read(resource).thenCompose(observed -> {
                    var fact = observed.map(value -> fact(resource, value))
                            .orElseThrow(
                                    () -> new IllegalStateException("native physical member inventory is missing"));
                    return read(resource).thenCompose(current -> {
                        if (!current.equals(observed)) {
                            return CompletableFuture.failedFuture(new IllegalStateException(
                                    "native physical member inventory changed during capture"));
                        }
                        return route.requireActiveResource(resource).thenApply(after -> {
                            if (!after.equals(binding)) {
                                throw new IllegalStateException("native physical member route changed during capture");
                            }
                            return fact;
                        });
                    });
                }));
    }

    /** A lost applied response is reconciled by exact append-only membership, including the original ticket Context. */
    public CompletionStage<Fact> register(
            PhysicalResourceIdV2 resource,
            Sha256Digest protocolCell,
            Member member,
            Sha256Digest capability,
            Sha256Digest ownerFence) {
        var initial = new Inventory(resource.sha256(), protocolCell, List.of(member));
        var proof = Sha256Digest.hash(initial.encode());
        var context =
                new Context(ProofBoundWriterClassV1.REFERENCE_SHARED_PHYSICAL_MEMBER_V1, capability, ownerFence, proof);
        return route.requireActiveResource(resource)
                .thenCompose(ignored -> read(resource))
                .thenCompose(observed -> {
                    var existing = observed.map(value -> fact(resource, value));
                    existing.ifPresent(value -> requireCell(value.inventory(), protocolCell));
                    if (existing.filter(value -> value.inventory().members().contains(member))
                            .isPresent()) {
                        return guard.reconcileTerminal(List.of(resource), context, proof)
                                .thenCompose(left -> {
                                    if (!left.isEmpty()) {
                                        return CompletableFuture.failedFuture(new IllegalStateException(
                                                "native physical member ticket cleanup is unresolved"));
                                    }
                                    return readFact(resource);
                                });
                    }
                    return guard.execute(List.of(resource), context, () -> append(resource, initial, member)
                                    .handle((value, failure) -> new Completion<>(
                                            new Registration(value, failure),
                                            failure == null ? Optional.of(proof) : Optional.empty())))
                            .thenApply(result -> {
                                if (!result.mutationInvoked()) {
                                    throw new IllegalStateException("native physical member admission was fenced");
                                }
                                var registration = result.value().orElseThrow();
                                if (registration.failure() != null) {
                                    throw new CompletionException(registration.failure());
                                }
                                if (!result.unresolvedTargets().isEmpty()) {
                                    throw new IllegalStateException(
                                            "native physical member ticket cleanup is unresolved");
                                }
                                return registration.fact();
                            });
                });
    }

    /** Metadata-only reconciliation after the original registration settled; never appends a missing member. */
    public CompletionStage<Fact> reconcileRegistration(
            PhysicalResourceIdV2 resource,
            Sha256Digest protocolCell,
            Member member,
            Sha256Digest capability,
            Sha256Digest ownerFence) {
        var proof = Sha256Digest.hash(new Inventory(resource.sha256(), protocolCell, List.of(member)).encode());
        var context =
                new Context(ProofBoundWriterClassV1.REFERENCE_SHARED_PHYSICAL_MEMBER_V1, capability, ownerFence, proof);
        return readFact(resource).thenCompose(original -> {
            requireCell(original.inventory(), protocolCell);
            if (!original.inventory().members().contains(member)) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("original native physical registration member remains unresolved"));
            }
            return guard.reconcileTerminal(List.of(resource), context, proof).thenCompose(left -> {
                if (!left.isEmpty()) {
                    return CompletableFuture.failedFuture(new IllegalStateException(
                            "original native physical registration ticket cleanup remains unresolved"));
                }
                return readFact(resource);
            });
        });
    }

    private CompletionStage<Fact> append(PhysicalResourceIdV2 resource, Inventory initial, Member member) {
        return read(resource).thenCompose(observed -> {
            var candidate = initial;
            if (observed.isPresent()) {
                var current = fact(resource, observed.orElseThrow()).inventory();
                requireCell(current, initial.protocolCell());
                if (current.members().contains(member)) {
                    return readFact(resource);
                }
                var members = new ArrayList<>(current.members());
                members.add(member);
                candidate = new Inventory(
                        resource.sha256(),
                        current.protocolCell(),
                        members.stream().sorted(ORDER).toList());
            }
            var mutation = observed.isEmpty()
                    ? client.createIfAbsent(nativeKey(resource), candidate.encode())
                    : client.compareAndSet(
                            nativeKey(resource),
                            candidate.encode(),
                            observed.orElseThrow().versionId());
            return mutation.handle((ignored, failure) -> null)
                    .thenCompose(ignored -> readFact(resource))
                    .thenApply(fact -> {
                        requireCell(fact.inventory(), initial.protocolCell());
                        if (!fact.inventory().members().contains(member)) {
                            throw new IllegalStateException("native physical member registration is unresolved");
                        }
                        return fact;
                    });
        });
    }

    private CompletionStage<Optional<AuthorityRecord>> read(PhysicalResourceIdV2 resource) {
        return client.read(nativeKey(resource));
    }

    private Fact fact(PhysicalResourceIdV2 resource, AuthorityRecord observed) {
        var inventory = Inventory.decode(observed.storedBytes());
        if (!observed.key().equals(nativeKey(resource)) || !inventory.resource().equals(resource.sha256())) {
            throw new IllegalArgumentException("native physical member inventory belongs to another resource");
        }
        return new Fact(
                inventory,
                new AuthorityFactV1(
                        observed.key(),
                        MetadataVersionMapper.fromOxia(observed.versionId()),
                        Sha256Digest.hash(observed.storedBytes())),
                observed.storedBytes());
    }

    private static void requireCell(Inventory inventory, Sha256Digest cell) {
        if (!inventory.protocolCell().equals(cell)) {
            throw new IllegalStateException("native physical member belongs to another Protocol Cell");
        }
    }

    private static Sha256Digest digest(ByteBuffer in) {
        byte[] value = new byte[32];
        in.get(value);
        return Sha256Digest.copyOf(value);
    }

    private static void requireDigest(Sha256Digest value, String name) {
        if (Objects.requireNonNull(value, name).isZero()) {
            throw new IllegalArgumentException(name + " is zero");
        }
    }
}

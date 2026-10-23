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

package com.nereusstream.metadata.oxia.v2.retention;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.metadata.oxia.v2.mutation.AsyncOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.BoundPhysicalDeleteAuthorityRouteV2;
import com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Entry;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Head;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Layout;
import com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteDoneV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.RangeScanConsumer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Admitted quota profile: native reservations precede all authority writes and terminal refunds use original keys.
 * The owner must exclude older/raw writers and assign one root per native physical namespace before initialization.
 * Canonical byte reservations do not reserve backend WAL, replica, filesystem or unrelated namespace capacity.
 */
public final class OxiaQuotaTargetDeleteStoreV2 implements BoundPhysicalDeleteAuthorityRouteV2 {
    private final AsyncOxiaClient scans;
    private final Layout layout;
    private final OxiaTargetDeleteAuthorityStoreV2 authorities;
    private final Accounting accounting;
    private final M5GcQuotaCoordinatorV2 quota;
    private final OxiaPhysicalMemberInventoryV2 members;
    private final java.util.function.Supplier<CompletionStage<PhysicalNamespaceAuthorityBindingV2>> bindingAuthority;

    public OxiaQuotaTargetDeleteStoreV2(
            AsyncOxiaClient client, Layout layout, ExactMetadataTransactionStoreV1 authoritativeFacts) {
        this(client, new AsyncOxiaConditionalClient(client), layout, authoritativeFacts);
    }

    /** Test delivery-fault seam; both ports must address the same actual native client and namespace. */
    OxiaQuotaTargetDeleteStoreV2(
            AsyncOxiaClient scans,
            OxiaConditionalClient conditional,
            Layout layout,
            ExactMetadataTransactionStoreV1 authoritativeFacts) {
        this(
                scans,
                conditional,
                layout,
                authoritativeFacts,
                () -> CompletableFuture.failedFuture(new UnsupportedOperationException(
                        "native physical namespace route admission is not installed")));
    }

    OxiaQuotaTargetDeleteStoreV2(
            AsyncOxiaClient scans,
            OxiaConditionalClient conditional,
            Layout layout,
            ExactMetadataTransactionStoreV1 authoritativeFacts,
            java.util.function.Supplier<CompletionStage<PhysicalNamespaceAuthorityBindingV2>> bindingAuthority) {
        this.scans = Objects.requireNonNull(scans, "scans");
        this.bindingAuthority = Objects.requireNonNull(bindingAuthority, "bindingAuthority");
        this.layout = Objects.requireNonNull(layout, "layout");
        authorities = new OxiaTargetDeleteAuthorityStoreV2(
                conditional, layout.nativeRoot(), layout.namespace(), authoritativeFacts);
        accounting = new Accounting(Objects.requireNonNull(conditional, "conditional"));
        quota = new M5GcQuotaCoordinatorV2(layout, accounting, authorities);
        members = new OxiaPhysicalMemberInventoryV2(
                conditional,
                this,
                new com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2(
                        new com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1(this)),
                layout.nativeRoot());
    }

    @Override
    public CompletionStage<PhysicalNamespaceAuthorityBindingV2> requireActiveResource(PhysicalResourceIdV2 resource) {
        layout.requireResource(resource);
        return currentRouteBinding()
                .thenCompose(
                        binding -> quota.requireExistingReservation(resource).thenCompose(grant -> {
                            if (grant.settled()) {
                                return CompletableFuture.failedFuture(
                                        new IllegalStateException("native resource quota is settled"));
                            }
                            return authorities.read(resource.authorityKey()).thenApply(value -> {
                                var stored = M5TargetDeleteStoredValueV2.decode(value.orElseThrow(() ->
                                        new IllegalStateException("reserved native resource has no active authority")));
                                if (stored.compactDone().isPresent()
                                        || stored.fullAuthority().orElseThrow().state()
                                                == com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                                        .TargetDeleteAuthorityStateV1.DELETE_DONE_V1) {
                                    throw new IllegalStateException("native resource authority is terminal");
                                }
                                return binding;
                            });
                        }))
                .thenCompose(binding -> currentRouteBinding().thenApply(current -> {
                    if (!current.equals(binding)) {
                        throw new IllegalStateException("native resource route changed during admission");
                    }
                    return current;
                }));
    }

    private CompletionStage<PhysicalNamespaceAuthorityBindingV2> currentRouteBinding() {
        return bindingAuthority.get().thenApply(binding -> {
            if (!binding.physicalNamespace().equals(layout.namespace())
                    || !binding.authorityRoot().equals(layout.nativeRoot())) {
                throw new IllegalStateException("native resource route differs from the permanent assignment");
            }
            return binding;
        });
    }

    Layout memberScanLayout() {
        members();
        return layout;
    }

    CompletionStage<PhysicalNamespaceAuthorityBindingV2> requireMemberScanRoute() {
        members();
        return currentRouteBinding();
    }

    /** Full registered permanent families; the caller retains its admitted workspace through observation use. */
    CompletionStage<java.util.Map<String, VersionedValue>> scanMemberNamespace(int maximumRows, long maximumBytes) {
        var captured = new java.util.TreeMap<String, VersionedValue>();
        var bytes = new long[1];
        CompletionStage<Void> stage = CompletableFuture.completedFuture(null);
        for (var range : permanentRecordRanges(memberScanLayout())) {
            stage = stage.thenCompose(ignored -> {
                var done = new CompletableFuture<Void>();
                scans.rangeScan(range.start(), range.end(), new RangeScanConsumer() {
                    private Throwable failure;

                    public boolean onNext(GetResult value) {
                        try {
                            String prefix = layout.nativeRoot() + "/";
                            if (!value.key().startsWith(prefix)) {
                                throw new IllegalArgumentException("physical namespace scan returned a foreign key");
                            }
                            layout.nativeKey(value.key().substring(prefix.length()));
                            if (captured.size() >= maximumRows
                                    || (bytes[0] = Math.addExact(bytes[0], value.value().length)) > maximumBytes) {
                                throw new IllegalArgumentException(
                                        "physical namespace scan exceeds its admitted bound");
                            }
                            var fact = VersionedValue.of(
                                    value.key(),
                                    CanonicalBytes.copyOf(value.value()),
                                    com.nereusstream.metadata.oxia.v2.mutation.MetadataVersionMapper.fromOxia(
                                            value.version().versionId()));
                            if (captured.putIfAbsent(fact.key(), fact) != null) {
                                throw new IllegalStateException("physical namespace scan repeated a native key");
                            }
                            return true;
                        } catch (Throwable error) {
                            failure = error;
                            return false;
                        }
                    }

                    public void onError(Throwable error) {
                        done.completeExceptionally(error);
                    }

                    public void onCompleted() {
                        if (failure == null) {
                            done.complete(null);
                        } else {
                            done.completeExceptionally(failure);
                        }
                    }
                });
                return done;
            });
        }
        return stage.thenApply(ignored -> java.util.Map.copyOf(captured));
    }

    public M5GcQuotaCoordinatorV2 quota() {
        return quota;
    }

    public OxiaPhysicalMemberInventoryV2 members() {
        if (layout.profile() != M5GcQuotaRecordsV2.RecordProfile.GC_AND_PHYSICAL_MEMBERS_V2) {
            throw new IllegalStateException("physical member accounting profile is not installed");
        }
        return members;
    }

    /** Actual same-resource predecessor facts. The mutable authority is not an eligibility freshness row. */
    public record ResourceFact(
            PhysicalResourceIdV2 resource,
            PhysicalNamespaceAuthorityBindingV2 binding,
            VersionedValue reservation,
            VersionedValue authority) {
        public ResourceFact {
            Objects.requireNonNull(resource, "resource");
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(reservation, "reservation");
            Objects.requireNonNull(authority, "authority");
            var layout = Layout.withPhysicalMembers(binding.authorityRoot(), binding.physicalNamespace());
            layout.requireResource(resource);
            var grant = Entry.decode(reservation.canonicalStoredBytes());
            layout.verify(grant);
            var stored = M5TargetDeleteStoredValueV2.decode(authority);
            if (!reservation.key().equals(M5GcQuotaRecordsV2.entryKey(resource.sha256()))
                    || !grant.resource().equals(resource.sha256())
                    || grant.settled()
                    || !authority.key().equals(resource.authorityKey())
                    || !stored.resource().equals(resource)
                    || stored.state()
                            == com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                    .TargetDeleteAuthorityStateV1.DELETE_DONE_V1) {
                throw new IllegalArgumentException("native physical resource facts differ or are terminal");
            }
        }
    }

    /** Reads the actual reservation/authority versions under the admitted native namespace, then rereads exactly. */
    public CompletionStage<ResourceFact> readResourceFact(PhysicalResourceIdV2 resource) {
        layout.requireResource(resource);
        String entryKey = M5GcQuotaRecordsV2.entryKey(resource.sha256());
        return requireActiveResource(resource)
                .thenCompose(binding -> accounting.read(entryKey).thenCompose(reservation -> authorities
                        .read(resource.authorityKey())
                        .thenApply(authority -> new ResourceFact(
                                resource,
                                binding,
                                reservation.orElseThrow(
                                        () -> new IllegalStateException("native reservation fact is missing")),
                                authority.orElseThrow(
                                        () -> new IllegalStateException("native authority fact is missing"))))))
                .thenCompose(fact -> accounting.read(entryKey).thenCompose(reservation -> {
                    if (!reservation.equals(Optional.of(fact.reservation()))) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("native physical reservation changed during fact capture"));
                    }
                    return authorities.read(resource.authorityKey()).thenCompose(authority -> {
                        if (!authority.equals(Optional.of(fact.authority()))) {
                            return CompletableFuture.failedFuture(
                                    new IllegalStateException("native physical authority changed during fact capture"));
                        }
                        return requireActiveResource(resource).thenApply(binding -> {
                            if (!binding.equals(fact.binding())) {
                                throw new IllegalStateException(
                                        "native physical namespace changed during fact capture");
                            }
                            return fact;
                        });
                    });
                }));
    }

    /** Initialize only empty typed families; never migrate old authority or implicitly expand capacity. */
    public CompletionStage<M5GcQuotaCoordinatorV2.Snapshot> initialize(long capacityBytes) {
        var initial = Head.empty(layout, capacityBytes);
        return accounting.read(M5GcQuotaRecordsV2.HEAD_KEY).thenCompose(existing -> {
            if (existing.isPresent()) {
                return quota.snapshot();
            }
            return accounting
                    .compareAndSet(Optional.empty(), M5GcQuotaRecordsV2.HEAD_KEY, initial.encode())
                    .thenCompose(ignored -> quota.snapshot());
        });
    }

    @Override
    public CompletionStage<Optional<VersionedValue>> read(String key) {
        return authorities.read(key);
    }

    @Override
    public CompletionStage<MutationOutcome> compareAndSet(
            Optional<VersionedValue> predecessor, String key, CanonicalBytes candidate) {
        Objects.requireNonNull(predecessor, "predecessor");
        var resource = M5TargetDeleteDoneV2.isCompactDone(candidate)
                ? M5TargetDeleteDoneV2.decode(candidate).resource()
                : M5TargetDeleteAuthorityCodecV1.decodeAuthority(candidate)
                        .target()
                        .resourceId();
        layout.requireResource(resource);
        if (!key.equals(resource.authorityKey())) {
            throw new IllegalArgumentException("quota authority mutation key differs from resource");
        }
        if (predecessor.isPresent()) {
            return quota.requireExistingReservation(resource)
                    .thenCompose(ignored -> authorities.compareAndSet(predecessor, key, candidate));
        }
        if (M5TargetDeleteDoneV2.isCompactDone(candidate)) {
            throw new IllegalArgumentException("compact done cannot create a new quota authority");
        }
        var initial = M5TargetDeleteAuthorityCodecV1.decodeAuthority(candidate);
        if (initial.state()
                        != com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                .TargetDeleteAuthorityStateV1.OPEN_V1
                || initial.authorityRevision() != 1
                || initial.predecessorAuthoritySha256().isPresent()) {
            throw new IllegalArgumentException("quota admission requires revision-one OPEN");
        }
        return quota.reserve(resource).thenCompose(result -> switch (result) {
            case GRANTED -> authorities.compareAndSet(predecessor, key, candidate);
            case SETTLED -> CompletableFuture.completedFuture(MutationOutcome.DEFINITIVE_CONFLICT);
            case EXHAUSTED -> CompletableFuture.completedFuture(MutationOutcome.PREDECESSOR_UNCHANGED);
            case RETRY -> CompletableFuture.completedFuture(MutationOutcome.RESPONSE_UNKNOWN);
        });
    }

    @Override
    public CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction transaction) {
        Objects.requireNonNull(transaction, "transaction");
        return CompletableFuture.completedFuture(TransactionOutcome.UNSUPPORTED);
    }

    @Override
    public boolean supportsAtomicMultiKeyTransactions() {
        return false;
    }

    /**
     * Locked common/compare/encode.go prefixes keys with total slash count. Each typed family therefore needs
     * same-depth bounds. A /v2... subtree scan misses deeper resource records even if the bounds end in slash.
     */
    static List<KeyRange> permanentRecordRanges(Layout layout) {
        String authority = layout.nativeRoot() + "/v2/physical-delete-m5-v2/";
        String entries = layout.nativeRoot() + "/" + M5GcQuotaRecordsV2.ENTRY_PREFIX;
        String births = layout.nativeRoot() + "/" + M5ObjectBirthCreditV3.PREFIX;
        var ranges = new java.util.ArrayList<KeyRange>();
        ranges.add(new KeyRange(
                authority + "0".repeat(64) + "/authority-v2", authority + "g".repeat(64) + "/authority-v2"));
        ranges.add(new KeyRange(entries + "0".repeat(64), entries + "g".repeat(64)));
        if (layout.namespace().providerKind() == PhysicalResourceIdV2.ProviderKind.OBJECT_PROVIDER) {
            ranges.add(new KeyRange(births + "0".repeat(64), births + "g".repeat(64)));
        }
        if (layout.profile() == M5GcQuotaRecordsV2.RecordProfile.GC_AND_PHYSICAL_MEMBERS_V2) {
            String members = layout.nativeRoot() + "/" + M5GcQuotaRecordsV2.PHYSICAL_MEMBER_PREFIX;
            ranges.add(new KeyRange(
                    members + "0".repeat(64) + "/inventory-v2", members + "g".repeat(64) + "/inventory-v2"));
        }
        return List.copyOf(ranges);
    }

    record KeyRange(String start, String end) {}

    private CompletionStage<Boolean> emptyRoot() {
        var ranges = permanentRecordRanges(layout);
        return accounting.read(M5GcQuotaRecordsV2.HEAD_KEY).thenCompose(head -> {
            if (head.isPresent()) {
                return CompletableFuture.completedFuture(false);
            }
            CompletionStage<Boolean> empty = CompletableFuture.completedFuture(true);
            for (var range : ranges) {
                empty = empty.thenCompose(
                        current -> current ? emptyRange(range) : CompletableFuture.completedFuture(false));
            }
            return empty;
        });
    }

    /** The locked client cancels on the first record; empty completion requires all native shards to complete. */
    private CompletionStage<Boolean> emptyRange(KeyRange range) {
        var result = new CompletableFuture<Boolean>();
        scans.rangeScan(range.start(), range.end(), new RangeScanConsumer() {
            private boolean empty = true;

            public boolean onNext(GetResult ignored) {
                empty = false;
                return false;
            }

            public void onError(Throwable failure) {
                result.completeExceptionally(failure);
            }

            public void onCompleted() {
                result.complete(empty);
            }
        });
        return result;
    }

    private final class Accounting implements ExactMetadataTransactionStoreV1 {
        private final Oxia09ExactMetadataTransactionStoreV1 exact;

        Accounting(OxiaConditionalClient client) {
            exact = new Oxia09ExactMetadataTransactionStoreV1(new OxiaConditionalClient() {
                public CompletionStage<Optional<AuthorityRecord>> read(String key) {
                    requireKey(key);
                    return client.read(layout.nativeKey(key))
                            .thenApply(observed -> observed.map(value -> {
                                if (!value.key().equals(layout.nativeKey(key))) {
                                    throw new IllegalArgumentException("native quota read returned another key");
                                }
                                verify(key, value.storedBytes());
                                return new AuthorityRecord(key, value.storedBytes(), value.versionId());
                            }));
                }

                public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes candidate) {
                    return client.createIfAbsent(layout.nativeKey(key), candidate);
                }

                public CompletionStage<Void> compareAndSet(String key, CanonicalBytes candidate, long version) {
                    return client.compareAndSet(layout.nativeKey(key), candidate, version);
                }
            });
        }

        public CompletionStage<Optional<VersionedValue>> read(String key) {
            requireKey(key);
            return exact.read(key);
        }

        public CompletionStage<MutationOutcome> compareAndSet(
                Optional<VersionedValue> predecessor, String key, CanonicalBytes candidate) {
            verify(key, candidate);
            predecessor.ifPresent(value -> {
                if (!value.key().equals(key)) {
                    throw new IllegalArgumentException("quota predecessor key differs");
                }
                verify(key, value.canonicalStoredBytes());
            });
            var admissible = key.equals(M5GcQuotaRecordsV2.HEAD_KEY)
                    ? headTransition(predecessor, Head.decode(candidate))
                    : key.startsWith(M5ObjectBirthCreditV3.PREFIX)
                            ? birthTransition(predecessor, M5ObjectBirthCreditV3.decode(candidate))
                            : entryTransition(predecessor, Entry.decode(candidate));
            return admissible.thenCompose(allowed -> allowed
                    ? exact.compareAndSet(predecessor, key, candidate)
                    : CompletableFuture.completedFuture(MutationOutcome.DEFINITIVE_CONFLICT));
        }

        public CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction transaction) {
            Objects.requireNonNull(transaction, "transaction");
            return CompletableFuture.completedFuture(TransactionOutcome.UNSUPPORTED);
        }

        public boolean supportsAtomicMultiKeyTransactions() {
            return false;
        }

        private CompletionStage<Boolean> headTransition(Optional<VersionedValue> predecessor, Head next) {
            if (predecessor.isEmpty()) {
                if (!next.equals(Head.empty(layout, next.capacityBytes()))) {
                    throw new IllegalArgumentException("quota initialization cannot import counters");
                }
                return emptyRoot();
            }
            var before = Head.decode(predecessor.orElseThrow().canonicalStoredBytes());
            if (next.capacityBytes() > before.capacityBytes() && next.equals(before.expand(next.capacityBytes()))) {
                return CompletableFuture.completedFuture(true);
            }
            if (before.pending().isPresent() && next.equals(before.clearPending())) {
                var pending = before.pending().orElseThrow();
                return read(pending.key()).thenApply(value -> value.filter(
                                entry -> entry.canonicalStoredBytes().equals(pending.encode()))
                        .isPresent());
            }
            var originalBirth = before.births().flatMap(M5GcQuotaRecordsV2.Births::pending);
            var pendingBirth = next.births().flatMap(M5GcQuotaRecordsV2.Births::pending);
            if (before.pending().isEmpty() && originalBirth.isPresent() && next.equals(before.clearPendingBirth())) {
                var credit = originalBirth.orElseThrow();
                return read(credit.key()).thenCompose(value -> {
                    if (value.filter(stored -> stored.canonicalStoredBytes().equals(credit.encode()))
                            .isEmpty()) {
                        return CompletableFuture.completedFuture(false);
                    }
                    if (credit.resource().isEmpty()) {
                        return CompletableFuture.completedFuture(true);
                    }
                    return read(M5GcQuotaRecordsV2.entryKey(
                                    credit.resource().orElseThrow().sha256()))
                            .thenApply(entry -> entry.map(stored -> Entry.decode(stored.canonicalStoredBytes()))
                                    .filter(stored -> !stored.settled())
                                    .isPresent());
                });
            }
            if (before.pending().isEmpty() && originalBirth.isEmpty() && pendingBirth.isPresent()) {
                return birthHeadTransition(before, next, pendingBirth.orElseThrow());
            }
            if (before.pending().isEmpty()
                    && originalBirth.isEmpty()
                    && next.pending().isPresent()) {
                var pending = next.pending().orElseThrow();
                if (!pending.settled() && next.equals(before.reserve(layout, pending.resource()))) {
                    return read(pending.key()).thenCompose(value -> {
                        if (value.isPresent()) {
                            return CompletableFuture.completedFuture(false);
                        }
                        return authorities
                                .read(M5GcQuotaRecordsV2.authorityKey(pending.resource()))
                                .thenApply(Optional::isEmpty);
                    });
                }
                if (pending.settled() && next.equals(before.settle(pending))) {
                    return read(pending.key()).thenCompose(value -> {
                        if (value.filter(entry -> entry.canonicalStoredBytes()
                                        .equals(pending.grant().encode()))
                                .isEmpty()) {
                            return CompletableFuture.completedFuture(false);
                        }
                        return exactDone(pending);
                    });
                }
            }
            throw new IllegalArgumentException("quota head does not follow a bounded exact transition");
        }

        private CompletionStage<Boolean> birthHeadTransition(Head before, Head next, M5ObjectBirthCreditV3 credit) {
            if (credit.state() == M5ObjectBirthCreditV3.State.RESERVED) {
                if (!next.equals(before.reserveBirth(layout, credit.worker(), credit.slot()))) {
                    throw new IllegalArgumentException("birth quota grant changes the original bounded head");
                }
                return read(credit.key()).thenApply(Optional::isEmpty);
            }
            return read(credit.key()).thenCompose(original -> {
                if (original.filter(value ->
                                credit.isSuccessorOf(M5ObjectBirthCreditV3.decode(value.canonicalStoredBytes())))
                        .isEmpty()) {
                    return CompletableFuture.completedFuture(false);
                }
                if (credit.resource().isEmpty()) {
                    return CompletableFuture.completedFuture(next.equals(
                            credit.state() == M5ObjectBirthCreditV3.State.WRITE_ARMED
                                    ? before.armBirth(credit)
                                    : before.finishBirth(credit, false)));
                }
                var resource = credit.resource().orElseThrow();
                return read(M5GcQuotaRecordsV2.entryKey(resource.sha256())).thenCompose(entry -> {
                    if (entry.map(value -> Entry.decode(value.canonicalStoredBytes())
                                            .settled())
                                    .orElse(false)
                            || !next.equals(before.finishBirth(credit, entry.isEmpty()))) {
                        return CompletableFuture.completedFuture(false);
                    }
                    return entry.isPresent()
                            ? CompletableFuture.completedFuture(true)
                            : authorities.read(resource.authorityKey()).thenApply(Optional::isEmpty);
                });
            });
        }

        private CompletionStage<Boolean> birthTransition(
                Optional<VersionedValue> predecessor, M5ObjectBirthCreditV3 next) {
            if (predecessor.isEmpty()
                    ? next.state() != M5ObjectBirthCreditV3.State.RESERVED
                    : !next.isSuccessorOf(M5ObjectBirthCreditV3.decode(
                            predecessor.orElseThrow().canonicalStoredBytes()))) {
                throw new IllegalArgumentException("birth credit requires its original permanent reserved predecessor");
            }
            return read(M5GcQuotaRecordsV2.HEAD_KEY)
                    .thenApply(head -> head.map(value -> Head.decode(value.canonicalStoredBytes()))
                            .filter(value -> value.pending().isEmpty())
                            .flatMap(value -> value.births().flatMap(M5GcQuotaRecordsV2.Births::pending))
                            .filter(next::equals)
                            .isPresent());
        }

        private CompletionStage<Boolean> entryTransition(Optional<VersionedValue> predecessor, Entry next) {
            if (predecessor.isEmpty()
                    ? next.settled()
                    : !Entry.decode(predecessor.orElseThrow().canonicalStoredBytes())
                                    .equals(next.grant())
                            || !next.settled()) {
                throw new IllegalArgumentException("permanent quota entry can only be granted then settled once");
            }
            return read(M5GcQuotaRecordsV2.HEAD_KEY).thenCompose(value -> {
                boolean pending = value.map(
                                head -> Head.decode(head.canonicalStoredBytes()).pending())
                        .orElse(Optional.empty())
                        .filter(next::equals)
                        .isPresent();
                if (!pending || !next.settled()) {
                    return CompletableFuture.completedFuture(pending);
                }
                return exactDone(next);
            });
        }

        private CompletionStage<Boolean> exactDone(Entry pending) {
            return authorities
                    .read(M5GcQuotaRecordsV2.authorityKey(pending.resource()))
                    .thenApply(value -> value.map(M5TargetDeleteStoredValueV2::decode)
                            .flatMap(M5TargetDeleteStoredValueV2::compactDone)
                            .filter(done -> pending.grant().settle(done).equals(pending))
                            .isPresent());
        }

        private void verify(String key, CanonicalBytes bytes) {
            requireKey(key);
            if (key.equals(M5GcQuotaRecordsV2.HEAD_KEY)) {
                layout.verify(Head.decode(bytes));
            } else if (key.startsWith(M5ObjectBirthCreditV3.PREFIX)) {
                var credit = M5ObjectBirthCreditV3.decode(bytes);
                if (!credit.key().equals(key) || !credit.scope().equals(layout.scope())) {
                    throw new IllegalArgumentException("birth credit key/scope differs from its original namespace");
                }
                credit.resource().ifPresent(layout::requireResource);
            } else {
                var entry = Entry.decode(bytes);
                layout.verify(entry);
                if (!entry.key().equals(key)) {
                    throw new IllegalArgumentException("quota entry key differs from permanent resource identity");
                }
            }
        }

        private void requireKey(String key) {
            Objects.requireNonNull(key, "key");
            if (!key.equals(M5GcQuotaRecordsV2.HEAD_KEY)
                    && !key.matches(M5GcQuotaRecordsV2.ENTRY_PREFIX + "[0-9a-f]{64}")
                    && !(layout.namespace().providerKind() == PhysicalResourceIdV2.ProviderKind.OBJECT_PROVIDER
                            && key.matches(M5ObjectBirthCreditV3.PREFIX + "[0-9a-f]{64}"))) {
                throw new IllegalArgumentException("key is outside the quota accounting family");
            }
            layout.nativeKey(key);
        }
    }
}

/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.GcReferenceDomain;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class GcReferenceDomainRegistryTest {
    private static final Checksum SHA_A = sha('a');
    private static final Checksum SHA_B = sha('b');
    private static final Checksum CRC = new Checksum(ChecksumType.CRC32C, "01020304");

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void closeScheduler() {
        scheduler.shutdownNow();
    }

    @Test
    void canonicalRegistryCollectsEveryExactDomainInCapabilityOrder() {
        GcReferenceQuery query = query();
        ArrayList<String> calls = new ArrayList<>();
        TestDomain projection = domain("projection-generation-v1", request -> {
            calls.add("projection-generation-v1");
            return clear(request, "projection-generation-v1", "/projection");
        });
        TestDomain generation = domain("generation-v1", request -> {
            calls.add("generation-v1");
            return clear(request, "generation-v1", "/generation");
        });
        GcReferenceDomainRegistry registry = new GcReferenceDomainRegistry(
                PhysicalGcConfig.defaults(), scheduler, List.of(projection, generation));

        GcReferenceCollection collection = registry.snapshotForDeletion(query).join();

        assertThat(registry.requiredDomains()).containsExactly(
                new GcReferenceDomainVersion("generation-v1", 1),
                new GcReferenceDomainVersion("projection-generation-v1", 1));
        assertThat(registry.contains("generation-v1", 1)).isTrue();
        assertThat(registry.contains("generation-v1", 2)).isFalse();
        assertThat(calls).containsExactly("generation-v1", "projection-generation-v1");
        assertThat(collection.status()).isEqualTo(GcReferenceCollectionStatus.CLEAR);
        assertThat(collection.snapshots()).extracting(GcReferenceSnapshot::domainId)
                .containsExactly("generation-v1", "projection-generation-v1");
        assertThat(collection.snapshot("projection-generation-v1")).isPresent();
    }

    @Test
    void vetoIncompleteAndConfiguredLimitAreDistinctFailClosedResults() {
        GcReferenceQuery query = query();

        GcReferenceCollection veto = registry(PhysicalGcConfig.defaults(), domain(
                        "generation-v1",
                        request -> GcReferenceSnapshot.create(
                                "generation-v1", 1, request.queryIdentitySha256(),
                                true, true, 1, 0,
                                List.of(authority("/a", 1)), List.of())))
                .snapshotForDeletion(query)
                .join();
        assertThat(veto.status()).isEqualTo(GcReferenceCollectionStatus.VETOED);
        assertThat(veto.blockingDomainId()).contains("generation-v1");

        GcReferenceCollection incomplete = registry(PhysicalGcConfig.defaults(), domain(
                        "generation-v1",
                        request -> GcReferenceSnapshot.create(
                                "generation-v1", 1, request.queryIdentitySha256(),
                                false, true, 2, 0,
                                List.of(authority("/a", 1)), List.of())))
                .snapshotForDeletion(query)
                .join();
        assertThat(incomplete.status()).isEqualTo(GcReferenceCollectionStatus.INCOMPLETE);

        PhysicalGcConfig oneAuthority = config(1, 10, Duration.ofSeconds(1));
        GcReferenceCollection oversized = registry(oneAuthority, domain(
                        "generation-v1",
                        request -> GcReferenceSnapshot.create(
                                "generation-v1", 1, request.queryIdentitySha256(),
                                true, false, 2, 0,
                                List.of(authority("/a", 1), authority("/b", 2)), List.of())))
                .snapshotForDeletion(query)
                .join();
        assertThat(oversized.status()).isEqualTo(GcReferenceCollectionStatus.LIMIT_EXCEEDED);

        assertThatThrownBy(() -> registry(PhysicalGcConfig.defaults(), domain(
                        "generation-v1", request -> clear(request, "generation-v1", "/a")))
                        .stillMatches(veto)
                        .join())
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void domainIdentityProtocolAndQueryMismatchAreInvariantFailures() {
        GcReferenceQuery query = query();
        TestDomain wrongDomain = domain(
                "generation-v1", request -> clear(request, "materialization-v1", "/a"));

        assertThatThrownBy(() -> registry(PhysicalGcConfig.defaults(), wrongDomain)
                        .snapshotForDeletion(query)
                        .join())
                .satisfies(failure -> assertThat(unwrap(failure))
                        .isInstanceOfSatisfying(NereusException.class, error ->
                                assertThat(error.code()).isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION)));

        GcReferenceQuery other = GcReferenceQuery.create(
                GcReferenceQueryKind.REFERENCED_OBJECT,
                query.object(),
                List.of(new StreamId("stream-b")),
                SHA_A);
        TestDomain wrongQuery = domain(
                "generation-v1", request -> clear(other, "generation-v1", "/a"));
        assertThatThrownBy(() -> registry(PhysicalGcConfig.defaults(), wrongQuery)
                        .snapshotForDeletion(query)
                        .join())
                .satisfies(failure -> assertThat(unwrap(failure))
                        .isInstanceOfSatisfying(NereusException.class, error ->
                                assertThat(error.code()).isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION)));
    }

    @Test
    void stillMatchesRevalidatesTheExactClearSetAndShortCircuitsOnDrift() {
        GcReferenceQuery query = query();
        TestDomain first = domain(
                "generation-v1", request -> clear(request, "generation-v1", "/a"));
        TestDomain second = domain(
                "projection-generation-v1",
                request -> clear(request, "projection-generation-v1", "/b"));
        second.stillMatches = false;
        GcReferenceDomainRegistry registry = new GcReferenceDomainRegistry(
                PhysicalGcConfig.defaults(), scheduler, List.of(second, first));
        GcReferenceCollection collection = registry.snapshotForDeletion(query).join();

        assertThat(registry.stillMatches(collection).join()).isFalse();
        assertThat(first.revalidationCalls.get()).isEqualTo(1);
        assertThat(second.revalidationCalls.get()).isEqualTo(1);

        first.stillMatches = false;
        second.revalidationCalls.set(0);
        assertThat(registry.stillMatches(collection).join()).isFalse();
        assertThat(second.revalidationCalls.get()).isZero();
    }

    @Test
    void everyDomainCallSharesOneBoundedOperationDeadline() {
        PhysicalGcConfig shortTimeout = config(10, 10, Duration.ofMillis(50));
        TestDomain never = new TestDomain(
                "generation-v1", 1, ignored -> new CompletableFuture<>());

        assertThatThrownBy(() -> registry(shortTimeout, never)
                        .snapshotForDeletion(query())
                        .join())
                .satisfies(failure -> assertThat(unwrap(failure))
                        .isInstanceOfSatisfying(NereusException.class, error ->
                                assertThat(error.code()).isEqualTo(ErrorCode.TIMEOUT)));
    }

    @Test
    void duplicateOrEmptyDomainRegistryIsRejected() {
        TestDomain first = domain(
                "generation-v1", request -> clear(request, "generation-v1", "/a"));
        TestDomain duplicate = domain(
                "generation-v1", request -> clear(request, "generation-v1", "/b"));

        assertThatThrownBy(() -> new GcReferenceDomainRegistry(
                        PhysicalGcConfig.defaults(), scheduler, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GcReferenceDomainRegistry(
                        PhysicalGcConfig.defaults(), scheduler, List.of(first, duplicate)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unique");
    }

    private GcReferenceDomainRegistry registry(PhysicalGcConfig config, GcReferenceDomain domain) {
        return new GcReferenceDomainRegistry(config, scheduler, List.of(domain));
    }

    private static TestDomain domain(
            String domainId, Function<GcReferenceQuery, GcReferenceSnapshot> snapshots) {
        return new TestDomain(
                domainId, 1, query -> CompletableFuture.completedFuture(snapshots.apply(query)));
    }

    private static GcReferenceSnapshot clear(
            GcReferenceQuery query, String domainId, String authorityKey) {
        return GcReferenceSnapshot.create(
                domainId,
                1,
                query.queryIdentitySha256(),
                true,
                false,
                1,
                0,
                List.of(authority(authorityKey, 1)),
                List.of());
    }

    private static GcAuthorityToken authority(String key, long version) {
        return new GcAuthorityToken(key, version, version == 1 ? SHA_A : SHA_B);
    }

    private static GcReferenceQuery query() {
        PhysicalObjectIdentity object = PhysicalObjectIdentity.create(
                new ObjectKey("objects/domain-registry"),
                Optional.empty(),
                PhysicalObjectKind.COMMITTED_COMPACTED,
                42,
                CRC,
                Optional.of(SHA_A),
                Optional.of("etag"));
        return GcReferenceQuery.create(
                GcReferenceQueryKind.REFERENCED_OBJECT,
                object,
                List.of(new StreamId("stream-a")),
                SHA_A);
    }

    private static PhysicalGcConfig config(
            int maxAuthorities, int maxReferences, Duration operationTimeout) {
        PhysicalGcConfig defaults = PhysicalGcConfig.defaults();
        return new PhysicalGcConfig(
                defaults.enabled(),
                defaults.dryRun(),
                defaults.metadataScanPageSize(),
                defaults.objectListPageSize(),
                defaults.maxConcurrentDeletes(),
                defaults.maxStreamsPerCandidate(),
                maxAuthorities,
                maxReferences,
                defaults.scanInterval(),
                defaults.readerLeaseDuration(),
                defaults.readerLeaseRenewInterval(),
                defaults.maximumClockSkew(),
                defaults.drainGrace(),
                defaults.pendingProtectionDuration(),
                defaults.orphanGrace(),
                defaults.tombstoneAuditGrace(),
                operationTimeout,
                defaults.closeTimeout());
    }

    private static Checksum sha(char value) {
        return new Checksum(ChecksumType.SHA256, Character.toString(value).repeat(64));
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static final class TestDomain implements GcReferenceDomain {
        private final String domainId;
        private final int protocolVersion;
        private final Function<GcReferenceQuery, CompletableFuture<GcReferenceSnapshot>> snapshots;
        private final AtomicInteger revalidationCalls = new AtomicInteger();
        private volatile boolean stillMatches = true;

        private TestDomain(
                String domainId,
                int protocolVersion,
                Function<GcReferenceQuery, CompletableFuture<GcReferenceSnapshot>> snapshots) {
            this.domainId = domainId;
            this.protocolVersion = protocolVersion;
            this.snapshots = snapshots;
        }

        @Override
        public String domainId() {
            return domainId;
        }

        @Override
        public int protocolVersion() {
            return protocolVersion;
        }

        @Override
        public CompletableFuture<GcReferenceSnapshot> snapshot(GcReferenceQuery query) {
            return snapshots.apply(query);
        }

        @Override
        public CompletableFuture<Boolean> stillMatches(
                GcReferenceQuery query, GcReferenceSnapshot snapshot) {
            if (!query.queryIdentitySha256().equals(snapshot.queryIdentitySha256())) {
                return CompletableFuture.failedFuture(
                        new IllegalArgumentException("query does not match snapshot"));
            }
            revalidationCalls.incrementAndGet();
            return CompletableFuture.completedFuture(stillMatches);
        }
    }
}

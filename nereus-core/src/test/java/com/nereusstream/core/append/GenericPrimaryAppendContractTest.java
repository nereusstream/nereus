/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.append;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.target.BookKeeperEntryMapping;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.metadata.oxia.CommitAppendRequest;
import com.nereusstream.metadata.oxia.CommittedAppend;
import com.nereusstream.metadata.oxia.MaterializedGenerationZero;
import com.nereusstream.metadata.oxia.PhysicalReferenceProof;
import com.nereusstream.metadata.oxia.PhysicalReferencePurpose;
import com.nereusstream.metadata.oxia.PreparedStableAppend;
import com.nereusstream.metadata.oxia.records.AppendSessionRecord;
import com.nereusstream.metadata.oxia.testing.FakeOxiaMetadataStore;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class GenericPrimaryAppendContractTest {
    private static final Checksum RANGE_SHA = new Checksum(
            ChecksumType.SHA256,
            "1111111111111111111111111111111111111111111111111111111111111111");
    private static final Checksum INDEX_SHA = new Checksum(
            ChecksumType.SHA256,
            "2222222222222222222222222222222222222222222222222222222222222222");

    @Test
    void commitsTaggedBookKeeperTarget() {
        String cluster = "generic-bk";
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(() -> 1_000L);
        StreamId streamId = new StreamId(metadata.createOrGetStream(
                cluster,
                new StreamName("events"),
                new StreamCreateOptions(StorageProfile.BOOKKEEPER_WAL_ONLY, Map.of()))
                .join()
                .streamId());
        AppendSessionRecord session = metadata.acquireAppendSession(
                cluster,
                streamId,
                new AppendSessionOptions("writer", Duration.ofSeconds(30), false))
                .join();
        BookKeeperEntryRangeReadTarget target = new BookKeeperEntryRangeReadTarget(
                1,
                "bk-a",
                17,
                9,
                1,
                BookKeeperEntryMapping.ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY,
                RANGE_SHA);
        CommitAppendRequest request = new CommitAppendRequest(
                streamId,
                "writer",
                "run",
                session.epoch(),
                session.fencingToken(),
                0,
                target,
                PayloadFormat.PULSAR_ENTRY_BATCH,
                1,
                1,
                7,
                List.of(),
                1,
                1,
                Optional.empty());

        PreparedStableAppend prepared = metadata.prepareStableAppend(cluster, request).join();
        assertThat(prepared.primaryTargetIdentitySha256()).isEqualTo(ReadTargetIdentities.sha256(target));
        assertThatThrownBy(prepared::objectKeyHash).isInstanceOf(IllegalStateException.class);

        PrimaryPhysicalReferenceAdapter<BookKeeperEntryRangeReadTarget> adapter =
                new SyntheticBookKeeperReferenceAdapter();
        DefaultObjectProtectionManager objectProtections = new DefaultObjectProtectionManager(
                cluster,
                metadata,
                Duration.ofMinutes(10),
                Duration.ZERO,
                Duration.ofHours(24),
                Clock.systemUTC());
        DefaultGenerationZeroPhysicalReferencePublisher publisher =
                new DefaultGenerationZeroPhysicalReferencePublisher(
                        cluster,
                        metadata,
                        metadata,
                        objectProtections,
                        List.of(adapter));

        ProtectedStableAppend protectedAppend = publisher.protectBeforeHead(
                prepared, Duration.ofSeconds(1)).join();
        assertThat(protectedAppend.proof().targetType())
                .isEqualTo(ReadTargetType.BOOKKEEPER_ENTRY_RANGE);

        CommittedAppend committed = new CommittedAppend(
                streamId,
                prepared.commitId(),
                "genesis",
                target,
                new OffsetRange(0, 1),
                0,
                7,
                1,
                PayloadFormat.PULSAR_ENTRY_BATCH,
                1,
                1,
                7,
                List.of(),
                Optional.empty(),
                1,
                1);
        MaterializedGenerationZero materialized = new MaterializedGenerationZero(
                committed,
                "/index/0",
                1,
                INDEX_SHA);
        ProtectedGenerationZero protectedGeneration = publisher.protectVisibleIndex(
                materialized, Duration.ofSeconds(1)).join();
        assertThat(protectedGeneration.proof().targetIdentitySha256())
                .isEqualTo(ReadTargetIdentities.sha256(target));
    }

    private record SyntheticBookKeeperProof(
            PhysicalReferencePurpose purpose,
            Checksum targetIdentitySha256,
            String referenceId) implements PhysicalReferenceProof {
        @Override
        public ReadTargetType targetType() {
            return ReadTargetType.BOOKKEEPER_ENTRY_RANGE;
        }
    }

    private static final class SyntheticBookKeeperReferenceAdapter
            implements PrimaryPhysicalReferenceAdapter<BookKeeperEntryRangeReadTarget> {
        @Override
        public ReadTargetType targetType() {
            return ReadTargetType.BOOKKEEPER_ENTRY_RANGE;
        }

        @Override
        public Class<BookKeeperEntryRangeReadTarget> targetClass() {
            return BookKeeperEntryRangeReadTarget.class;
        }

        @Override
        public CompletableFuture<ProtectedStableAppend> protectBeforeHead(
                PreparedStableAppend append,
                BookKeeperEntryRangeReadTarget target,
                Duration timeout) {
            return CompletableFuture.completedFuture(new ProtectedStableAppend(
                    append,
                    new SyntheticBookKeeperProof(
                            PhysicalReferencePurpose.REACHABLE_APPEND,
                            ReadTargetIdentities.sha256(target),
                            GenerationZeroProtectionIdentities.reachableAppendReferenceId(append))));
        }

        @Override
        public CompletableFuture<ProtectedGenerationZero> protectVisibleIndex(
                MaterializedGenerationZero append,
                BookKeeperEntryRangeReadTarget target,
                Duration timeout) {
            return CompletableFuture.completedFuture(new ProtectedGenerationZero(
                    append,
                    new SyntheticBookKeeperProof(
                            PhysicalReferencePurpose.VISIBLE_GENERATION,
                            ReadTargetIdentities.sha256(target),
                            GenerationZeroProtectionIdentities.visibleGenerationReferenceId(append))));
        }
    }
}

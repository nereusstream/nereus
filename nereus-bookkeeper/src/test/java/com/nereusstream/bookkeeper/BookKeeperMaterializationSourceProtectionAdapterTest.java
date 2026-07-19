/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.core.wal.DurablePrimaryAppend;
import com.nereusstream.materialization.MaterializationSourceProtection;
import com.nereusstream.materialization.SourceGeneration;
import com.nereusstream.metadata.oxia.CommitAppendRequest;
import com.nereusstream.metadata.oxia.CommittedAppend;
import com.nereusstream.metadata.oxia.MaterializedGenerationZero;
import com.nereusstream.metadata.oxia.PreparedStableAppend;
import com.nereusstream.metadata.oxia.records.BookKeeperProtectionType;
import com.nereusstream.metadata.oxia.records.ProtectionLifecycle;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class BookKeeperMaterializationSourceProtectionAdapterTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Test
    void walRuntimeExportsOneMatchedMaterializationSourceProvider() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime =
                new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            BookKeeperMaterializationSourceProtectionAdapter adapter = adapter(runtime);
            BookKeeperWalRuntime walRuntime = new BookKeeperWalRuntime(
                    runtime.appender, runtime.reader, runtime.references);

            var provider = walRuntime.materializationSourceProvider(adapter);

            assertThat(provider.reader()).isSameAs(runtime.reader);
            assertThat(provider.protectionAdapter()).isSameAs(adapter);
        }
    }

    @Test
    void acquiresTransfersRevalidatesAndReleasesExactDynamicSlot() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime =
                new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            SourceFixture fixture = source(runtime);
            BookKeeperMaterializationSourceProtectionAdapter adapter = adapter(runtime);
            ObjectProtectionOwner claimed = owner("/tasks/task-a", 0, 'e');
            AtomicInteger claimRevalidations = new AtomicInteger();

            MaterializationSourceProtection acquired = adapter.acquireOrTransfer(
                            BookKeeperPrimaryWalAppenderTest.STREAM,
                            fixture.source(),
                            "materialization-source-a",
                            claimed,
                            expected -> {
                                assertThat(expected).isEqualTo(claimed);
                                claimRevalidations.incrementAndGet();
                                return CompletableFuture.completedFuture(null);
                            })
                    .join();
            var durable = acquired.requireProviderHandle(
                            BookKeeperMaterializationSourceProtectionHandle.class)
                    .protection();
            assertThat(durable.value().protectionSlot()).isGreaterThanOrEqualTo(3);
            assertThat(durable.value().protectionType())
                    .isEqualTo(BookKeeperProtectionType.MATERIALIZATION_SOURCE);
            assertThat(durable.value().lifecycle()).isEqualTo(ProtectionLifecycle.ACTIVE);
            assertThat(durable.value().ownerMetadataVersion()).isZero();
            assertThatCode(() -> new BookKeeperProtectionRetirementProof(
                            durable.key(),
                            durable.metadataVersion(),
                            durable.durableValueSha256(),
                            durable.value().ownerKey(),
                            durable.value().ownerMetadataVersion(),
                            sha('e'),
                            "/authority/higher-generation",
                            0,
                            sha('f'),
                            BookKeeperProtectionRetirementProof.Reason.HEALTHY_HIGHER_GENERATION))
                    .doesNotThrowAnyException();
            assertThat(claimRevalidations).hasValueGreaterThanOrEqualTo(2);

            MaterializationSourceProtection replay = adapter.acquireOrTransfer(
                            BookKeeperPrimaryWalAppenderTest.STREAM,
                            fixture.source(),
                            acquired.referenceId(),
                            claimed,
                            expected -> CompletableFuture.completedFuture(null))
                    .join();
            assertThat(replay.metadataVersion()).isEqualTo(acquired.metadataVersion());
            assertThat(replay.providerHandle()).isEqualTo(acquired.providerHandle());
            assertThat(adapter.findExisting(
                            BookKeeperPrimaryWalAppenderTest.STREAM,
                            fixture.source(),
                            acquired.referenceId())
                    .join()).contains(replay);

            ObjectProtectionOwner outputReady = owner("/tasks/task-a", 2, 'f');
            MaterializationSourceProtection transferred = adapter.transfer(
                            replay,
                            outputReady,
                            expected -> {
                                assertThat(expected).isEqualTo(outputReady);
                                return CompletableFuture.completedFuture(null);
                            })
                    .join();
            assertThat(transferred.owner()).isEqualTo(outputReady);
            assertThat(transferred.metadataVersion()).isGreaterThan(replay.metadataVersion());
            assertThat(adapter.revalidate(
                            transferred,
                            expected -> CompletableFuture.completedFuture(null))
                    .join()).isEqualTo(transferred);

            AtomicInteger releaseAuthorizations = new AtomicInteger();
            adapter.release(transferred, exact -> {
                        assertThat(exact).isEqualTo(transferred);
                        releaseAuthorizations.incrementAndGet();
                        return CompletableFuture.completedFuture(null);
                    })
                    .join();
            assertThat(releaseAuthorizations).hasValue(1);
            assertThat(runtime.metadata.getProtection(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            runtime.configuration.providerScopeSha256(),
                            fixture.target().ledgerId(),
                            durable.value().ledgerRangeSlot(),
                            durable.value().protectionSlot())
                    .join()).isEmpty();
            assertThat(adapter.findExisting(
                            BookKeeperPrimaryWalAppenderTest.STREAM,
                            fixture.source(),
                            acquired.referenceId())
                    .join()).isEmpty();
        }
    }

    @Test
    void fixedDynamicSlotsRejectBeforeSourceIoWhenEverySlotIsOwned() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime =
                new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            SourceFixture fixture = source(runtime);
            BookKeeperMaterializationSourceProtectionAdapter adapter = adapter(runtime);
            List<MaterializationSourceProtection> acquired = new ArrayList<>();
            int dynamicSlots = runtime.configuration.protectionSlotsPerRange() - 3;
            for (int index = 0; index < dynamicSlots; index++) {
                ObjectProtectionOwner owner = owner("/tasks/task-" + index, index, 'a');
                acquired.add(adapter.acquireOrTransfer(
                                BookKeeperPrimaryWalAppenderTest.STREAM,
                                fixture.source(),
                                "materialization-source-" + index,
                                owner,
                                expected -> CompletableFuture.completedFuture(null))
                        .join());
            }
            assertThat(acquired).extracting(value -> value
                            .requireProviderHandle(BookKeeperMaterializationSourceProtectionHandle.class)
                            .protection()
                            .value()
                            .protectionSlot())
                    .doesNotHaveDuplicates();

            ObjectProtectionOwner overflowOwner = owner("/tasks/task-overflow", 7, 'b');
            assertThatThrownBy(() -> adapter.acquireOrTransfer(
                            BookKeeperPrimaryWalAppenderTest.STREAM,
                            fixture.source(),
                            "materialization-source-overflow",
                            overflowOwner,
                            expected -> CompletableFuture.completedFuture(null))
                    .join())
                    .rootCause()
                    .isInstanceOf(NereusException.class)
                    .extracting(error -> ((NereusException) error).code())
                    .isEqualTo(ErrorCode.BACKPRESSURE_REJECTED);
        }
    }

    private static SourceFixture source(BookKeeperPrimaryWalAppenderTest.Runtime runtime) {
        var session = BookKeeperPrimaryWalAppenderTest.session();
        DurablePrimaryAppend durable;
        try (BookKeeperPreparedPrimaryAppend prepared = runtime.appender.prepare(
                BookKeeperPrimaryWalAppenderTest.request(
                        session, "attempt-materialization-source", 0, new byte[] {1}, new byte[] {2}))) {
            durable = runtime.appender.persist(prepared, TIMEOUT).join();
        }
        BookKeeperEntryRangeReadTarget target =
                (BookKeeperEntryRangeReadTarget) durable.readTarget();
        CommitAppendRequest commit = new CommitAppendRequest(
                BookKeeperPrimaryWalAppenderTest.STREAM,
                session.writerId(),
                "generic-process-run",
                session.epoch(),
                session.fencingToken(),
                0,
                target,
                PayloadFormat.OPAQUE_RECORD_BATCH,
                2,
                2,
                2,
                List.of(),
                1,
                2,
                Optional.empty());
        PreparedStableAppend prepared = new PreparedStableAppend(
                commit,
                commit.commitId(),
                "/commits/materialization-source",
                7,
                sha('c'),
                ReadTargetIdentities.sha256(target),
                false);
        runtime.references.protectBeforeHead(prepared, target, TIMEOUT).join();
        CommittedAppend committed = new CommittedAppend(
                BookKeeperPrimaryWalAppenderTest.STREAM,
                prepared.commitId(),
                "",
                target,
                new OffsetRange(0, 2),
                0,
                2,
                1,
                PayloadFormat.OPAQUE_RECORD_BATCH,
                2,
                2,
                2,
                List.of(),
                Optional.empty(),
                1,
                2);
        MaterializedGenerationZero generationZero = new MaterializedGenerationZero(
                committed, "/indexes/materialization-source", 0, sha('d'));
        runtime.references.protectVisibleIndex(generationZero, target, TIMEOUT).join();
        SourceGeneration source = new SourceGeneration(
                ReadView.COMMITTED,
                committed.range(),
                0,
                committed.commitVersion(),
                generationZero.indexKey(),
                generationZero.indexMetadataVersion(),
                generationZero.indexRecordSha256(),
                target,
                ReadTargetIdentities.sha256(target),
                Optional.empty(),
                committed.payloadFormat(),
                committed.projectionRef(),
                committed.recordCount(),
                committed.entryCount(),
                committed.logicalBytes(),
                committed.schemaRefs(),
                0,
                committed.cumulativeSize());
        return new SourceFixture(target, source);
    }

    private static BookKeeperMaterializationSourceProtectionAdapter adapter(
            BookKeeperPrimaryWalAppenderTest.Runtime runtime) {
        return new BookKeeperMaterializationSourceProtectionAdapter(
                BookKeeperPrimaryWalAppenderTest.CLUSTER,
                runtime.configuration,
                runtime.metadata,
                BookKeeperPrimaryWalAppenderTest.CLOCK);
    }

    private static ObjectProtectionOwner owner(String key, long version, char identity) {
        return new ObjectProtectionOwner(key, version, sha(identity));
    }

    private static Checksum sha(char value) {
        return new Checksum(ChecksumType.SHA256, Character.toString(value).repeat(64));
    }

    private record SourceFixture(
            BookKeeperEntryRangeReadTarget target,
            SourceGeneration source) {
    }
}
